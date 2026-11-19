import { writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { DeploymentError, WorkerDeployment } from '../src/index';
import { EventReader, openBroker, producerBundle, recordingLogger, sqliteSpec, waitFor, workDir, writeBundle } from './helpers';

const cleanups: (() => Promise<void> | void)[] = [];

afterEach(async () => {
  while (cleanups.length > 0) await cleanups.pop()!();
});

async function setup(prefix: string) {
  const dir = workDir(prefix);
  const broker = await openBroker(dir.db);
  const reader = new EventReader(broker);
  const logger = recordingLogger();
  cleanups.push(dir.cleanup, () => broker.stop(), () => reader.close());
  return { ...dir, broker, reader, logger };
}

function track(deployment: WorkerDeployment): WorkerDeployment {
  cleanups.push(async () => {
    await deployment.stop();
  });
  return deployment;
}

describe('WorkerDeployment', () => {
  it('runs a bundle in its own worker, reports its contents and stops cleanly', async () => {
    const env = await setup('worker-basic');
    const path = writeBundle(env.deployDir, 'orders.js', producerBundle('v1'));
    const deployment = track(new WorkerDeployment(path, { connector: sqliteSpec(env.db), logger: env.logger, runner: { receiveTimeoutMs: 50 } }));
    await deployment.start({});
    expect(deployment.info()).toMatchObject({ name: 'orders.js', kind: 'file', status: 'running', consumers: ['OrderConsumer'], services: [], configFiles: [], generation: 1, file: path });
    expect(deployment.isAlive()).toBe(true);
    expect(deployment.threadId()).toBeGreaterThan(0);
    await env.broker.getQueueProducer('orders').send('o-1');
    await env.reader.waitFor((events) => events.length === 1);
    expect(env.reader.events).toEqual([{ version: 'v1', queue: 'orders', body: 'o-1' }]);
    expect(await deployment.stop()).toEqual({ drained: true });
    expect(deployment.isAlive()).toBe(false);
    expect(deployment.exitCount()).toBe(1);
    expect(deployment.info().status).toBe('stopped');
    expect(env.broker.openConsumers()).toBe(1);
  });

  it('applies the properties snapshot to @Config injection', async () => {
    const env = await setup('worker-config');
    const path = writeBundle(
      env.deployDir,
      'config.js',
      `
      import { Config, OnMessage, Producer, Queue, type MessageProducer } from '@integratetech/core';
      @Queue('config')
      export class ConfigConsumer {
        @Config('app', 'greeting') greeting!: string;
        @Producer({ queue: 'events' }) events!: MessageProducer;
        @OnMessage()
        async handle() { await this.events.send(JSON.stringify({ greeting: this.greeting ?? null })); }
      }
      `,
    );
    const deployment = track(new WorkerDeployment(path, { connector: sqliteSpec(env.db), logger: env.logger }));
    await deployment.start({ app: { greeting: 'hello' } });
    expect(deployment.configFiles()).toEqual(['app']);
    await env.broker.getQueueProducer('config').send('x');
    await env.reader.waitFor((events) => events.length === 1);
    expect(env.reader.events).toEqual([{ greeting: 'hello' }]);
  });

  it('reports a bundle that fails to import without crashing the caller', async () => {
    const env = await setup('worker-broken');
    const path = join(env.deployDir, 'broken.js');
    writeFileSync(path, 'export class {');
    const deployment = track(new WorkerDeployment(path, { connector: sqliteSpec(env.db), logger: env.logger }));
    const error = await deployment.start({}).catch((caught: unknown) => caught);
    expect(error).toBeInstanceOf(DeploymentError);
    expect((error as Error).message).toContain('Unable to start deployment broken.js');
    expect(deployment.info().status).toBe('failed');
    expect(deployment.isAlive()).toBe(false);
    expect(env.logger.text('error')).toContain('[broken.js] load failed');
  });

  it('reports a bundle without consumers', async () => {
    const env = await setup('worker-empty');
    const path = join(env.deployDir, 'empty.js');
    writeFileSync(path, 'export const answer = 42;\n');
    const deployment = track(new WorkerDeployment(path, { connector: sqliteSpec(env.db), logger: env.logger }));
    await expect(deployment.start({})).rejects.toThrow('Bundle empty.js exports no consumer classes');
    expect(deployment.isAlive()).toBe(false);
  });

  it('reports a connector that cannot be created', async () => {
    const env = await setup('worker-connector');
    const path = writeBundle(env.deployDir, 'orders.js', producerBundle('v1'));
    const deployment = track(new WorkerDeployment(path, { connector: { module: '@integratetech/connector-sqlite', export: 'Missing' }, logger: env.logger }));
    await expect(deployment.start({})).rejects.toThrow('has no class export named Missing');
  });

  it('marks a worker that crashes at runtime as failed and notifies', async () => {
    const env = await setup('worker-crash');
    const path = writeBundle(
      env.deployDir,
      'crash.js',
      `
      import { OnMessage, Queue } from '@integratetech/core';
      @Queue('crash')
      export class CrashConsumer {
        @OnMessage()
        handle() { setTimeout(() => { throw new Error('worker exploded'); }, 0); }
      }
      `,
    );
    const failures: string[] = [];
    const deployment = track(
      new WorkerDeployment(path, { connector: sqliteSpec(env.db), logger: env.logger, onFailure: (_failed, error) => failures.push(error) }),
    );
    await deployment.start({});
    await env.broker.getQueueProducer('crash').send('boom');
    await waitFor(() => failures.length === 1);
    expect(failures[0]).toContain('worker exploded');
    expect(deployment.info()).toMatchObject({ status: 'failed' });
    expect(deployment.isAlive()).toBe(false);
    expect(await deployment.stop()).toEqual({ drained: true });
    expect(deployment.info().status).toBe('failed');
  });

  it('force-terminates a worker whose drain runs past the timeout', async () => {
    const env = await setup('worker-timeout');
    const path = writeBundle(
      env.deployDir,
      'stuck.js',
      `
      import { OnMessage, Producer, Queue, type MessageProducer } from '@integratetech/core';
      @Queue('stuck')
      export class StuckConsumer {
        @Producer({ queue: 'events' }) events!: MessageProducer;
        @OnMessage()
        async handle() {
          await this.events.send(JSON.stringify({ phase: 'start' }));
          await new Promise((resolve) => setTimeout(resolve, 60_000));
        }
      }
      `,
    );
    const deployment = track(new WorkerDeployment(path, { connector: sqliteSpec(env.db), logger: env.logger, drainTimeoutMs: 100 }));
    await deployment.start({});
    await env.broker.getQueueProducer('stuck').send('x');
    await env.reader.waitFor((events) => events.length === 1);
    const startedAt = Date.now();
    expect(await deployment.stop()).toEqual({ drained: false });
    expect(Date.now() - startedAt).toBeLessThan(5_000);
    expect(deployment.isAlive()).toBe(false);
    expect(env.logger.text('warn')).toContain('force-terminated');
  });

  it('can be started again after it stops', async () => {
    const env = await setup('worker-restart');
    const path = writeBundle(env.deployDir, 'orders.js', producerBundle('v1'));
    const deployment = track(new WorkerDeployment(path, { connector: sqliteSpec(env.db), logger: env.logger }));
    await deployment.start({});
    await expect(deployment.start({})).rejects.toThrow('already running');
    await deployment.stop();
    await deployment.start({});
    expect(deployment.info()).toMatchObject({ status: 'running', generation: 2 });
    expect(deployment.exitCount()).toBe(1);
  });
});
