import { existsSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { type DeploymentEvent, DeploymentManager, type DeploymentManagerOptions, WorkerDeployment } from '../src/index';
import { EventReader, openBroker, producerBundle, recordingLogger, sqliteSpec, waitFor, workDir, writeAtomic, writeBundle } from './helpers';

const cleanups: (() => Promise<void> | void)[] = [];

afterEach(async () => {
  while (cleanups.length > 0) await cleanups.pop()!();
});

const slowBundle = (version: string) => `
import { Message, OnMessage, Producer, Queue, type MessageProducer } from '@integratetech/core';

@Queue('slow')
export class SlowConsumer {
  @Message() body!: string;
  @Producer({ queue: 'events' }) events!: MessageProducer;

  @OnMessage()
  async handle() {
    await this.events.send(JSON.stringify({ version: '${version}', phase: 'start', body: this.body }));
    await new Promise((resolve) => setTimeout(resolve, 600));
    await this.events.send(JSON.stringify({ version: '${version}', phase: 'end', body: this.body }));
  }
}
`;

const configBundle = `
import { Config, OnMessage, Producer, Queue, type MessageProducer } from '@integratetech/core';

@Queue('config')
export class ConfigConsumer {
  @Config('app', 'greeting') greeting!: string;
  @Producer({ queue: 'events' }) events!: MessageProducer;

  @OnMessage()
  async handle() {
    await this.events.send(JSON.stringify({ greeting: this.greeting ?? null }));
  }
}
`;

async function setup(prefix: string, options: Partial<DeploymentManagerOptions> = {}) {
  const dir = workDir(prefix);
  const broker = await openBroker(dir.db);
  const reader = new EventReader(broker);
  const logger = recordingLogger();
  const events: DeploymentEvent[] = [];
  const manager = new DeploymentManager({
    deployDir: dir.deployDir,
    propertiesDir: dir.propertiesDir,
    connector: sqliteSpec(dir.db),
    pollIntervalMs: 60_000,
    settleMs: 10,
    runner: { receiveTimeoutMs: 50 },
    logger,
    ...options,
  });
  manager.on((event) => events.push(event));
  cleanups.push(dir.cleanup, () => broker.stop(), () => reader.close(), () => manager.stop());
  const settle = async () => {
    await manager.scan();
    await manager.scan();
  };
  return { ...dir, broker, reader, logger, manager, events, settle };
}

const workerOf = (manager: DeploymentManager, name: string) => manager.get(name) as WorkerDeployment;

describe('DeploymentManager with file deployments', () => {
  it('deploys existing bundles on start, then redeploys and undeploys on change', async () => {
    const env = await setup('manager-lifecycle');
    writeBundle(env.deployDir, 'orders.js', producerBundle('v1'));
    await env.manager.start();
    expect(env.manager.list()).toMatchObject([{ name: 'orders.js', status: 'running', generation: 1 }]);
    await env.broker.getQueueProducer('orders').send('a');
    await env.reader.waitFor((events) => events.length === 1);

    const first = workerOf(env.manager, 'orders.js');
    writeBundle(env.deployDir, 'orders.js', producerBundle('v2-longer'));
    await env.settle();
    const second = workerOf(env.manager, 'orders.js');
    expect(second).not.toBe(first);
    expect(first.isAlive()).toBe(false);
    expect(second.isAlive()).toBe(true);
    await env.broker.getQueueProducer('orders').send('b');
    await env.reader.waitFor((events) => events.length === 2);
    expect(env.reader.events.map((event) => event.version)).toEqual(['v1', 'v2-longer']);

    rmSync(join(env.deployDir, 'orders.js'));
    await env.manager.scan();
    expect(env.manager.list()).toEqual([]);
    expect(second.isAlive()).toBe(false);
    expect(env.events.map((event) => event.type)).toEqual(['deployed', 'redeployed', 'undeployed']);
  });

  it('finishes in-flight messages before swapping to the new version', async () => {
    const env = await setup('manager-drain');
    writeBundle(env.deployDir, 'slow.js', slowBundle('v1'));
    await env.manager.start();
    await env.broker.getQueueProducer('slow').send('m1');
    await env.reader.waitFor((events) => events.length === 1);
    writeBundle(env.deployDir, 'slow.js', slowBundle('v2-new'));
    await env.settle();
    await env.broker.getQueueProducer('slow').send('m2');
    await env.reader.waitFor((events) => events.length === 4);
    expect(env.reader.events.map((event) => `${event.version}:${event.phase}:${event.body}`)).toEqual([
      'v1:start:m1',
      'v1:end:m1',
      'v2-new:start:m2',
      'v2-new:end:m2',
    ]);
    expect(env.events.find((event) => event.type === 'redeployed')).toMatchObject({ drained: true, reason: 'bundle' });
    expect(await env.broker.depth('slow')).toBe(0);
  });

  it('loads properties before consumers start and restarts readers when a file changes', async () => {
    const env = await setup('manager-properties');
    writeAtomic(join(env.propertiesDir, 'app.properties'), 'greeting=hello\n');
    writeBundle(env.deployDir, 'config.js', configBundle);
    writeBundle(env.deployDir, 'orders.js', producerBundle('v1'));
    await env.manager.start();
    expect(env.manager.propertiesManager.readersOf('app')).toEqual(['config.js']);
    await env.broker.getQueueProducer('config').send('1');
    await env.reader.waitFor((events) => events.length === 1);

    const orders = workerOf(env.manager, 'orders.js');
    writeAtomic(join(env.propertiesDir, 'app.properties'), 'greeting=bonjour\n');
    await env.settle();
    expect(env.events.filter((event) => event.type === 'properties' && event.affected.length > 0)).toEqual([{ type: 'properties', changed: ['app'], affected: ['config.js'] }]);
    expect(env.manager.get('config.js')!.info().generation).toBe(2);
    expect(workerOf(env.manager, 'orders.js')).toBe(orders);
    expect(orders.info().generation).toBe(1);
    await env.broker.getQueueProducer('config').send('2');
    await env.reader.waitFor((events) => events.length === 2);

    rmSync(join(env.propertiesDir, 'app.properties'));
    await env.manager.scan();
    await env.broker.getQueueProducer('config').send('3');
    await env.reader.waitFor((events) => events.length === 3);
    expect(env.reader.events).toEqual([{ greeting: 'hello' }, { greeting: 'bonjour' }, { greeting: null }]);
    expect(env.events.find((event) => event.type === 'redeployed')).toMatchObject({ name: 'config.js', reason: 'properties' });
  });

  it('skips a broken bundle, keeps the others running and recovers when it is fixed', async () => {
    const env = await setup('manager-broken');
    writeFileSync(join(env.deployDir, 'broken.js'), 'export class {');
    writeBundle(env.deployDir, 'orders.js', producerBundle('ok'));
    await env.manager.start();
    expect(env.manager.list().map((info) => `${info.name}:${info.status}`)).toEqual(['broken.js:failed', 'orders.js:running']);
    expect(env.events.filter((event) => event.type === 'failed').map((event) => event.name)).toEqual(['broken.js']);
    await env.broker.getQueueProducer('orders').send('still-works');
    await env.reader.waitFor((events) => events.length === 1);

    writeBundle(env.deployDir, 'broken.js', producerBundle('fixed', 'fixed'));
    await env.settle();
    expect(env.manager.get('broken.js')!.info().status).toBe('running');
    await env.broker.getQueueProducer('fixed').send('now');
    await env.reader.waitFor((events) => events.length === 2);
    expect(env.reader.events[1]).toEqual({ version: 'fixed', queue: 'fixed', body: 'now' });
  });

  it('isolates a crashing worker from the other deployments', async () => {
    const env = await setup('manager-crash');
    writeBundle(
      env.deployDir,
      'crash.js',
      `
      import { OnMessage, Queue } from '@integratetech/core';
      @Queue('crash')
      export class CrashConsumer {
        @OnMessage()
        handle() { setTimeout(() => { throw new Error('crashed hard'); }, 0); }
      }
      `,
    );
    writeBundle(env.deployDir, 'orders.js', producerBundle('alive'));
    await env.manager.start();
    await env.broker.getQueueProducer('crash').send('x');
    await waitFor(() => env.events.some((event) => event.type === 'failed'));
    expect(env.events.find((event) => event.type === 'failed')).toMatchObject({ name: 'crash.js' });
    expect(env.manager.get('crash.js')!.info()).toMatchObject({ status: 'failed' });
    expect(workerOf(env.manager, 'orders.js').isAlive()).toBe(true);
    await env.broker.getQueueProducer('orders').send('after-crash');
    await env.reader.waitFor((events) => events.length === 1);
  });

  it('terminates every worker on stop', async () => {
    const env = await setup('manager-stop');
    writeBundle(env.deployDir, 'a.js', producerBundle('a', 'a'));
    writeBundle(env.deployDir, 'b.js', producerBundle('b', 'b'));
    await env.manager.start();
    const workers = ['a.js', 'b.js'].map((name) => workerOf(env.manager, name));
    expect(workers.every((worker) => worker.isAlive())).toBe(true);
    expect(new Set(workers.map((worker) => worker.threadId())).size).toBe(2);
    await env.manager.stop();
    expect(workers.map((worker) => [worker.isAlive(), worker.exitCount()])).toEqual([
      [false, 1],
      [false, 1],
    ]);
    expect(env.manager.list()).toEqual([]);
    expect(env.events.filter((event) => event.type === 'undeployed').map((event) => event.name).sort()).toEqual(['a.js', 'b.js']);
  });

  it('polls on its own timer', async () => {
    const env = await setup('manager-poll', { pollIntervalMs: 20 });
    await env.manager.start();
    writeBundle(env.deployDir, 'late.js', producerBundle('late', 'late'));
    await waitFor(() => env.manager.get('late.js')?.info().status === 'running');
    await env.broker.getQueueProducer('late').send('hi');
    await env.reader.waitFor((events) => events.length === 1);
  });

  it('loads lifecycle plugins inside each worker from module specs', async () => {
    const env = await setup('manager-plugins');
    const logFile = join(env.root, 'steps.log');
    const pluginPath = writeBundle(
      env.root,
      'step-log-plugin.js',
      `
      import { appendFileSync } from 'node:fs';
      import { LifeCycle, ProcessStep, type MessageRunnable } from '@integratetech/core';
      export class StepLogPlugin {
        constructor(private readonly options: { file: string; tag: string }) {}
        private log(runnable: MessageRunnable, step: string) { appendFileSync(this.options.file, this.options.tag + ':' + step + ':' + runnable.msg.body + '\\n'); }
        @LifeCycle(ProcessStep.Pending) pending(runnable: MessageRunnable) { this.log(runnable, 'Pending'); }
        @LifeCycle(ProcessStep.Validating) validating(runnable: MessageRunnable) { this.log(runnable, 'Validating'); }
        @LifeCycle(ProcessStep.Processing) processing(runnable: MessageRunnable) { this.log(runnable, 'Processing'); }
        @LifeCycle(ProcessStep.Complete) complete(runnable: MessageRunnable) { this.log(runnable, 'Complete'); }
      }
      `,
    );
    const plugins = [{ module: pluginPath, export: 'StepLogPlugin', options: { file: logFile, tag: 'worker' } }];
    const manager = new DeploymentManager({ deployDir: env.deployDir, connector: sqliteSpec(env.db), plugins, pollIntervalMs: 60_000, settleMs: 10, logger: env.logger });
    cleanups.push(() => manager.stop());
    writeBundle(
      env.deployDir,
      'audited.js',
      `
      import { OnMessage, Queue } from '@integratetech/core';
      @Queue('audited')
      export class AuditedConsumer {
        @OnMessage()
        handle() {}
      }
      `,
    );
    await manager.start();
    await env.broker.getQueueProducer('audited').send('payload');
    await waitFor(async () => (await env.broker.depth('audited')) === 0 && existsSync(logFile) && readFileSync(logFile, 'utf8').includes('Complete'));
    await manager.stop();
    expect(readFileSync(logFile, 'utf8').trim().split('\n')).toEqual(['worker:Pending:payload', 'worker:Validating:payload', 'worker:Processing:payload', 'worker:Complete:payload']);
  });
});
