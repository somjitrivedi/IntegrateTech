import { describe, expect, it } from 'vitest';
import {
  Consumer,
  Header,
  IntegrateMessage,
  LifeCycle,
  Message,
  type MessageRunnable,
  OnMessage,
  ProcessStep,
  Properties,
  Queue,
  RoleInput,
  RoleInputMessage,
  RoleInputSource,
  createAnnotation,
  roleQueueName,
} from '../src/index';
import { runUntilIdle, startIntegrate } from './helpers';

const Resendable = createAnnotation<{ role: string }>('role-test-resendable');
const processed: { id?: string; body: string; props: Record<string, string> }[] = [];
let failNext = true;

@Queue('payments')
@RoleInput('PaymentConsumer')
@Resendable({ role: 'PaymentConsumer' })
class PaymentConsumer {
  @Message('message') msg!: IntegrateMessage;
  @Message() body!: string;
  @Properties() props!: Record<string, string>;

  @OnMessage()
  handle() {
    processed.push({ id: this.msg.id, body: this.body, props: { ...this.props } });
    if (failNext) {
      failNext = false;
      throw new Error('downstream unavailable');
    }
  }
}

class ResendOnError {
  @LifeCycle(ProcessStep.Error, Resendable)
  async resend(runnable: MessageRunnable, resendable: { role: string }) {
    const connector = runnable.consumer.integrate.getConnector();
    const producer = connector.getQueueProducer(roleQueueName(resendable.role));
    const envelope = RoleInputMessage.fromMessage(resendable.role, runnable.msg);
    await producer.send(envelope.toString(), { properties: { reason: 'retry' } });
    await producer.close();
  }
}

@RoleInput()
class DefaultRole {
  @OnMessage()
  handle() {}
}

describe('RoleInputMessage', () => {
  it('round trips through json', () => {
    const message = new IntegrateMessage('body');
    message.id = 'abc';
    message.setProperties({ a: '1' });
    message.setHeaders({ [Header.RetryCount]: 2 });
    const envelope = RoleInputMessage.fromMessage('Role', message);
    const parsed = RoleInputMessage.fromString(envelope.toString());
    expect(parsed.equals(envelope)).toBe(true);
    expect(parsed).toMatchObject({ role: 'Role', id: 'abc', msg: 'body', properties: { a: '1' }, headers: { [Header.RetryCount]: 2 } });
  });

  it('rejects malformed input and tolerates missing optional fields', () => {
    expect(() => RoleInputMessage.fromString('nope')).toThrow('Could not deserialize RoleInputMessage from JSON: nope');
    expect(() => RoleInputMessage.fromString('[1]')).toThrow('Could not deserialize');
    const sparse = RoleInputMessage.fromString('{"role":"R","properties":{"n":5,"skip":null},"headers":{"Bogus":1}}');
    expect(sparse.id).toBeUndefined();
    expect(sparse.properties).toEqual({ n: '5' });
    expect(sparse.headers).toEqual({});
  });

  it('delegates acknowledgement to the wrapping broker message', async () => {
    let acked = 0;
    class Source extends IntegrateMessage {
      override async acknowledge() {
        acked++;
        return true;
      }
      override async beforeProcessing() {
        return false;
      }
    }
    const unwrapped = new RoleInputMessage('R', 'id-1', 'inner', null, null).toMessage(new Source('outer'));
    expect(unwrapped.id).toBe('id-1');
    expect(unwrapped.body).toBe('inner');
    expect(await unwrapped.acknowledge()).toBe(true);
    expect(await unwrapped.beforeProcessing()).toBe(false);
    expect(acked).toBe(1);
    expect(await new RoleInputMessage('R', undefined, undefined).toMessage().acknowledge()).toBe(true);
  });

  it('defaults the role to the consumer class name', () => {
    const source = new RoleInputSource().setContextClass(DefaultRole);
    expect(source.getRole()).toBe('DefaultRole');
  });
});

describe('role targeted re-send', () => {
  it('re-sends a failed message to only this consumer and keeps its identity', async () => {
    processed.length = 0;
    failNext = true;
    const { integrate, connector } = await startIntegrate();
    integrate.lifeCycle.register(new ResendOnError());
    const id = await connector.getQueueProducer('payments').send('{"amount":10}', { properties: { customer: 'c-1' } });
    const [queueConsumer, roleConsumer] = integrate.createConsumers(PaymentConsumer, { receiveTimeoutMs: 30 }) as [Consumer, Consumer];
    expect(roleConsumer.getSource()).toBeInstanceOf(RoleInputSource);
    await runUntilIdle(queueConsumer);
    expect(await connector.depth(roleQueueName('PaymentConsumer'))).toBe(1);
    await runUntilIdle(roleConsumer);
    expect(processed).toEqual([
      { id, body: '{"amount":10}', props: { customer: 'c-1' } },
      { id, body: '{"amount":10}', props: { customer: 'c-1' } },
    ]);
    expect(await connector.depth('payments')).toBe(0);
    expect(await connector.depth(roleQueueName('PaymentConsumer'))).toBe(0);
    await integrate.shutdown();
  });
});
