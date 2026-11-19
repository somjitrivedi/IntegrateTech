import { describe, expect, it } from 'vitest';
import {
  Consumer,
  type HandlerClass,
  IntegrateMessage,
  LifeCycle,
  type MessageRunnable,
  On,
  OnMessage,
  OnValidate,
  ProcessStep,
  Producer,
  type MessageProducer,
  Queue,
} from '../src/index';
import { runUntilIdle, startIntegrate } from './helpers';

const calls: string[] = [];

class OutcomeRecorder {
  static outcomes: { body?: string; outcome?: string; errors: string[] }[] = [];

  @LifeCycle(ProcessStep.Complete)
  @LifeCycle(ProcessStep.Invalid)
  @LifeCycle(ProcessStep.Error)
  static record(runnable: MessageRunnable) {
    OutcomeRecorder.outcomes.push({ body: runnable.msg.body, outcome: runnable.outcome, errors: [...runnable.errors] });
  }
}

@Queue('runnable')
class Complete {
  @OnValidate()
  listCheck() {
    calls.push('validate-list');
    return [];
  }

  @OnValidate()
  async boolCheck() {
    calls.push('validate-bool');
    return true;
  }

  @OnMessage()
  first() {
    calls.push('message-1');
  }

  @OnMessage()
  async second() {
    await Promise.resolve();
    calls.push('message-2');
  }

  @On(ProcessStep.Validating)
  onValidating() {
    calls.push('hook-validating');
  }

  @On(ProcessStep.Processing)
  onProcessing() {
    calls.push('hook-processing');
  }

  @On(ProcessStep.Complete)
  onComplete() {
    calls.push('hook-complete');
  }
}

@Queue('runnable')
class InvalidList {
  @OnValidate()
  check() {
    return ['missing name', 'missing age'];
  }

  @OnMessage()
  handle() {
    calls.push('should-not-run');
  }

  @On(ProcessStep.Invalid)
  invalid() {
    calls.push('hook-invalid');
  }
}

@Queue('runnable')
class InvalidBoolean {
  @OnValidate()
  isGood() {
    return false;
  }
}

@Queue('runnable')
class WrongReturnType {
  @OnValidate()
  weird() {
    return 'nope';
  }
}

@Queue('runnable')
class ThrowingValidator {
  @OnValidate()
  explode(): boolean {
    throw new Error('validator blew up');
  }
}

@Queue('runnable')
class ThrowingHandler {
  @OnMessage()
  handle() {
    throw new Error('processing failed');
  }

  @On(ProcessStep.Error, ProcessStep.Invalid)
  oops() {
    calls.push('hook-error');
  }
}

@Queue('runnable')
class ThrowingHook {
  @OnMessage()
  handle() {}

  @On(ProcessStep.Validating)
  brokenHook() {
    throw new Error('hook failed');
  }
}

const producers: MessageProducer[] = [];

@Queue('runnable')
class UsesProducer {
  @Producer({ queue: 'downstream' })
  out!: MessageProducer;

  @OnMessage()
  async handle() {
    producers.push(this.out);
    await this.out.send('forwarded');
  }
}

async function process(handler: HandlerClass, body = 'payload') {
  calls.length = 0;
  OutcomeRecorder.outcomes = [];
  const { integrate, connector, logger } = await startIntegrate();
  integrate.lifeCycle.register(OutcomeRecorder);
  await connector.getQueueProducer('runnable').send(body);
  const consumer = new Consumer(handler, integrate, { receiveTimeoutMs: 30 });
  await runUntilIdle(consumer);
  const remaining = await connector.depth('runnable');
  await integrate.shutdown();
  return { outcome: OutcomeRecorder.outcomes[0], remaining, logger, connector };
}

describe('MessageRunnable lifecycle', () => {
  it('validates, processes, runs hooks in order and acknowledges', async () => {
    const { outcome, remaining } = await process(Complete);
    expect(outcome).toEqual({ body: 'payload', outcome: ProcessStep.Complete, errors: [] });
    expect(calls).toEqual(['hook-validating', 'validate-list', 'validate-bool', 'hook-processing', 'message-1', 'message-2', 'hook-complete']);
    expect(remaining).toBe(0);
  });

  it('routes validator error lists to Invalid without processing', async () => {
    const { outcome, remaining, logger } = await process(InvalidList);
    expect(outcome!.outcome).toBe(ProcessStep.Invalid);
    expect(outcome!.errors).toEqual(['missing name', 'missing age']);
    expect(calls).toEqual(['hook-invalid']);
    expect(remaining).toBe(0);
    expect(logger.text('error')).toContain('missing name; missing age');
  });

  it('treats a false validator as invalid', async () => {
    const { outcome } = await process(InvalidBoolean);
    expect(outcome!.outcome).toBe(ProcessStep.Invalid);
    expect(outcome!.errors).toEqual(['Validator isGood returned false']);
  });

  it('reports validators with the wrong return type', async () => {
    const { outcome } = await process(WrongReturnType);
    expect(outcome!.outcome).toBe(ProcessStep.Invalid);
    expect(outcome!.errors).toEqual(['onValidate method weird has wrong return type string']);
  });

  it('captures validator exceptions as validation errors', async () => {
    const { outcome } = await process(ThrowingValidator);
    expect(outcome!.outcome).toBe(ProcessStep.Invalid);
    expect(outcome!.errors).toEqual(['validator blew up']);
  });

  it('routes handler exceptions to Error and still acknowledges', async () => {
    const { outcome, remaining } = await process(ThrowingHandler);
    expect(outcome!.outcome).toBe(ProcessStep.Error);
    expect(outcome!.errors).toEqual(['processing failed']);
    expect(calls).toEqual(['hook-error']);
    expect(remaining).toBe(0);
  });

  it('records hook failures as errors', async () => {
    const { outcome } = await process(ThrowingHook);
    expect(outcome!.outcome).toBe(ProcessStep.Invalid);
    expect(outcome!.errors).toEqual(['hook failed']);
  });

  it('closes injected producers after the message completes', async () => {
    producers.length = 0;
    const { integrate, connector } = await startIntegrate();
    await connector.getQueueProducer('runnable').send('x');
    await runUntilIdle(new Consumer(UsesProducer, integrate, { receiveTimeoutMs: 30 }));
    expect(producers).toHaveLength(1);
    await expect(producers[0]!.send('late')).rejects.toThrow('Producer is closed');
    expect(await connector.depth('downstream')).toBe(1);
    await integrate.shutdown();
  });

  it('skips processing and does not acknowledge when beforeProcessing declines', async () => {
    calls.length = 0;
    const { integrate } = await startIntegrate();
    const consumer = new Consumer(Complete, integrate);
    class Declined extends IntegrateMessage {
      acked = false;
      override async beforeProcessing() {
        return false;
      }
      override async acknowledge() {
        this.acked = true;
        return true;
      }
    }
    const message = new Declined('x');
    const runner = await consumer.prepare(message);
    expect(await runner.run()).toBe('Skipped');
    expect(message.acked).toBe(false);
    expect(calls).toEqual([]);
    await integrate.shutdown();
  });

  it('logs failed acknowledgements', async () => {
    const { integrate, logger } = await startIntegrate();
    const consumer = new Consumer(Complete, integrate);
    class Unackable extends IntegrateMessage {
      override async acknowledge() {
        return false;
      }
    }
    await (await consumer.prepare(new Unackable('x'))).run();
    expect(logger.text('error')).toContain('Failed to acknowledge message after processing');
    await integrate.shutdown();
  });
});
