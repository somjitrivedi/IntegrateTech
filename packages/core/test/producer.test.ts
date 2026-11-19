import { describe, expect, it } from 'vitest';
import { AbstractProducer, Header, IntegrateMessage, type OutboundMessage, ProducerError, sanitizeHeaders } from '../src/index';

class CapturingProducer extends AbstractProducer {
  readonly sent: OutboundMessage[] = [];
  fail = false;
  closeCalls = 0;

  protected async publish(message: OutboundMessage): Promise<void> {
    if (this.fail) throw new Error('broker down');
    this.sent.push(message);
  }

  protected override async onClose(): Promise<void> {
    this.closeCalls++;
  }
}

describe('AbstractProducer', () => {
  it('generates a 32 character id and stamps correlation id and timestamp', async () => {
    const producer = new CapturingProducer();
    const id = await producer.send('x');
    expect(id).toMatch(/^[0-9a-f]{32}$/);
    expect(producer.sent[0]!.headers[Header.CorrelationId]).toBe(id);
    expect(typeof producer.sent[0]!.headers[Header.Timestamp]).toBe('number');
    expect(producer.sent[0]!.priority).toBe(4);
    expect(producer.sent[0]!.expiresAt).toBeNull();
  });

  it('prefers the message id, then the correlation id header', async () => {
    const producer = new CapturingProducer();
    const message = new IntegrateMessage('x');
    message.id = 'mine';
    expect(await producer.send(message)).toBe('mine');
    expect(await producer.send('y', { headers: { [Header.CorrelationId]: 'corr' } })).toBe('corr');
  });

  it('clamps priority, maps expiration and applies delay', async () => {
    const producer = new CapturingProducer();
    await producer.send('a', { headers: { [Header.Priority]: 42, [Header.Expiration]: 1234 }, delayMs: 50 });
    await producer.send('b', { headers: { [Header.Priority]: -3 }, delayMs: -10 });
    expect(producer.sent[0]!.priority).toBe(9);
    expect(producer.sent[0]!.expiresAt).toBe(1234);
    expect(producer.sent[0]!.delayMs).toBe(50);
    expect(producer.sent[1]!.priority).toBe(0);
    expect(producer.sent[1]!.delayMs).toBe(0);
  });

  it('serializes objects and rejects unserializable bodies', async () => {
    const producer = new CapturingProducer();
    await producer.send({ a: [1, 2] });
    expect(producer.sent[0]!.body).toBe('{"a":[1,2]}');
    const circular: Record<string, unknown> = {};
    circular.self = circular;
    await expect(producer.send(circular)).rejects.toBeInstanceOf(ProducerError);
    await expect(producer.send(new IntegrateMessage())).rejects.toThrow('Message body is required');
  });

  it('wraps publish failures and closes once', async () => {
    const producer = new CapturingProducer();
    producer.fail = true;
    const error = await producer.send('x').catch((caught: unknown) => caught);
    expect(error).toBeInstanceOf(ProducerError);
    expect((error as ProducerError).message).toBe('Failed to send message');
    expect(((error as ProducerError).cause as Error).message).toBe('broker down');
    await producer.close();
    await producer.close();
    expect(producer.closeCalls).toBe(1);
    expect(producer.isClosed()).toBe(true);
  });

  it('drops unknown headers and coerces numeric ones', () => {
    expect(sanitizeHeaders({ Priority: '7', Bogus: 'x', Type: 3, RetryCount: 'NaN-ish' })).toEqual({ Priority: 7, Type: '3' });
  });
});
