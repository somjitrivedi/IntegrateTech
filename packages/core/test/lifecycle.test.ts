import { describe, expect, it } from 'vitest';
import {
  Consumer,
  LifeCycle,
  LifeCycleMonitors,
  type MessageRunnable,
  On,
  OnMessage,
  OnValidate,
  ProcessStep,
  Queue,
  createAnnotation,
} from '../src/index';
import { recordingLogger, runUntilIdle, startIntegrate } from './helpers';

const Audited = createAnnotation<{ label: string }>('lifecycle-test-audited');

class StaticAuditor {
  static readonly seen: string[] = [];

  @LifeCycle(ProcessStep.Pending)
  @LifeCycle(ProcessStep.Validating)
  @LifeCycle(ProcessStep.Processing)
  @LifeCycle(ProcessStep.Complete)
  @LifeCycle(ProcessStep.Invalid)
  @LifeCycle(ProcessStep.Error)
  static record(runnable: MessageRunnable) {
    StaticAuditor.seen.push(`${runnable.msg.body}:${runnable.outcome ?? 'running'}`);
  }
}

class InstanceAuditor {
  readonly steps: ProcessStep[] = [];

  @LifeCycle(ProcessStep.Validating)
  validating() {
    this.steps.push(ProcessStep.Validating);
  }

  @LifeCycle(ProcessStep.Complete)
  async complete() {
    await Promise.resolve();
    this.steps.push(ProcessStep.Complete);
  }
}

class AnnotationAuditor {
  readonly labels: string[] = [];

  @LifeCycle(ProcessStep.Error, Audited)
  onError(_runnable: MessageRunnable, audited: { label: string }) {
    this.labels.push(audited.label);
  }
}

class BadAuditor {
  constructor() {
    throw new Error('cannot build');
  }

  @LifeCycle(ProcessStep.Pending)
  never() {}
}

class ThrowingAuditor {
  @LifeCycle(ProcessStep.Processing)
  static boom() {
    throw new Error('plugin exploded');
  }
}

@Queue('lifecycle-ok')
class Healthy {
  @OnMessage()
  handle() {}
}

@Queue('lifecycle-fail')
@Audited({ label: 'failing-consumer' })
class Failing {
  @OnMessage()
  handle() {
    throw new Error('handler failed');
  }
}

@Queue('lifecycle-plain-fail')
class PlainFailing {
  @OnMessage()
  handle() {
    throw new Error('handler failed');
  }
}

@Queue('lifecycle-invalid')
class Rejecting {
  @OnValidate()
  check() {
    return false;
  }

  @On(ProcessStep.Invalid)
  invalidHook() {}
}

describe('LifeCycleMonitors', () => {
  it('calls static monitors for every step a message passes through', async () => {
    StaticAuditor.seen.length = 0;
    const { integrate, connector } = await startIntegrate();
    integrate.lifeCycle.register(StaticAuditor);
    await connector.getQueueProducer('lifecycle-ok').send('ok');
    await connector.getQueueProducer('lifecycle-invalid').send('bad');
    await runUntilIdle(new Consumer(Healthy, integrate, { receiveTimeoutMs: 30 }));
    await runUntilIdle(new Consumer(Rejecting, integrate, { receiveTimeoutMs: 30 }));
    expect(StaticAuditor.seen).toEqual([
      'ok:running',
      'ok:running',
      'ok:running',
      'ok:Complete',
      'bad:running',
      'bad:running',
      'bad:Invalid',
    ]);
    await integrate.shutdown();
  });

  it('awaits async instance monitors registered as instances', async () => {
    const { integrate, connector } = await startIntegrate();
    const auditor = new InstanceAuditor();
    integrate.lifeCycle.register(auditor);
    await connector.getQueueProducer('lifecycle-ok').send('x');
    await runUntilIdle(new Consumer(Healthy, integrate, { receiveTimeoutMs: 30 }));
    expect(auditor.steps).toEqual([ProcessStep.Validating, ProcessStep.Complete]);
    await integrate.shutdown();
  });

  it('only calls annotation scoped monitors for annotated consumers and passes the value', async () => {
    const { integrate, connector } = await startIntegrate();
    const auditor = new AnnotationAuditor();
    integrate.lifeCycle.register(auditor);
    await connector.getQueueProducer('lifecycle-fail').send('a');
    await connector.getQueueProducer('lifecycle-plain-fail').send('b');
    await runUntilIdle(new Consumer(Failing, integrate, { receiveTimeoutMs: 30 }));
    await runUntilIdle(new Consumer(PlainFailing, integrate, { receiveTimeoutMs: 30 }));
    expect(auditor.labels).toEqual(['failing-consumer']);
    await integrate.shutdown();
  });

  it('skips monitors that cannot be constructed and isolates monitors that throw', async () => {
    const { integrate, connector, logger } = await startIntegrate();
    expect(integrate.lifeCycle.register(BadAuditor)).toBe(false);
    expect(logger.text('error')).toContain('Unable to instantiate lifecycle monitor BadAuditor');
    integrate.lifeCycle.register(ThrowingAuditor);
    const auditor = new InstanceAuditor();
    integrate.lifeCycle.register(auditor);
    await connector.getQueueProducer('lifecycle-ok').send('x');
    const consumer = new Consumer(Healthy, integrate, { receiveTimeoutMs: 30 });
    await runUntilIdle(consumer);
    expect(consumer.processedCount()).toBe(1);
    expect(auditor.steps).toEqual([ProcessStep.Validating, ProcessStep.Complete]);
    expect(logger.text('error')).toContain('plugin exploded');
    expect(await connector.depth('lifecycle-ok')).toBe(0);
    await integrate.shutdown();
  });

  it('deregisters every monitor of a class', () => {
    const monitors = new LifeCycleMonitors(recordingLogger());
    monitors.register(StaticAuditor);
    monitors.register(new InstanceAuditor());
    expect(monitors.count()).toBe(8);
    monitors.deregister(StaticAuditor);
    expect(monitors.count()).toBe(2);
    expect(monitors.count(ProcessStep.Pending)).toBe(0);
    expect(monitors.register(class Empty {})).toBe(false);
    expect(monitors.getCalls()).toBe(0);
  });
});
