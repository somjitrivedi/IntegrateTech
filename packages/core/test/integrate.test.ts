import { MemoryConnector } from '@integratetech/connector-memory';
import { describe, expect, it } from 'vitest';
import { type Connector, ConnectorError, IntegrateTech, LifeCycle, ProcessStep, StartupError, consoleLogger, describeError, silentLogger } from '../src/index';
import { recordingLogger } from './helpers';

describe('IntegrateTech', () => {
  it('starts and stops its connector once', async () => {
    const integrate = new IntegrateTech({ logger: silentLogger });
    const connector = new MemoryConnector();
    expect(() => integrate.getConnector()).toThrow(ConnectorError);
    await integrate.start(connector);
    expect(integrate.isRunning()).toBe(true);
    expect(integrate.getConnector()).toBe(connector);
    expect(integrate.getLifeCycle()).toBe(integrate.lifeCycle);
    await integrate.shutdown();
    await integrate.shutdown();
    expect(integrate.isRunning()).toBe(false);
    expect(connector.isStarted()).toBe(false);
  });

  it('wraps connector start failures in a StartupError', async () => {
    const integrate = new IntegrateTech({ logger: silentLogger });
    const failing = { start: async () => Promise.reject(new Error('no broker')) } as unknown as Connector;
    const error = await integrate.start(failing).catch((caught: unknown) => caught);
    expect(error).toBeInstanceOf(StartupError);
    expect((error as Error).message).toBe('no broker');
    expect(integrate.isRunning()).toBe(false);
  });

  it('logs connector stop failures instead of throwing', async () => {
    const logger = recordingLogger();
    const integrate = new IntegrateTech({ logger });
    const flaky = { start: async () => {}, stop: async () => Promise.reject(new Error('stuck')) } as unknown as Connector;
    await integrate.start(flaky);
    await integrate.shutdown();
    expect(logger.text('error')).toContain('Error stopping connector: stuck');
  });

  it('uses the console logger by default and describes nested errors', () => {
    expect(new IntegrateTech().logger).toBe(consoleLogger);
    expect(describeError(new Error('outer', { cause: new Error('inner') }))).toBe('inner');
    expect(describeError('plain')).toBe('plain');
  });

  it('starts plugins once, stops them in reverse order and logs stop failures', async () => {
    const logger = recordingLogger();
    const integrate = new IntegrateTech({ logger });
    await integrate.start(new MemoryConnector());
    const order: string[] = [];
    class Listening {
      @LifeCycle(ProcessStep.Complete)
      complete() {}
      start(owner: IntegrateTech) {
        order.push(owner === integrate ? 'start:listening' : 'wrong');
      }
      stop() {
        order.push('stop:listening');
      }
    }
    class Quiet {
      start() {
        order.push('start:quiet');
      }
      stop() {
        order.push('stop:quiet');
        throw new Error('stop failed');
      }
    }
    const listening = new Listening();
    await integrate.registerPlugin(listening);
    await integrate.registerPlugin(listening);
    await integrate.registerPlugin(new Quiet());
    expect(integrate.lifeCycle.count(ProcessStep.Complete)).toBe(1);
    expect(logger.text('warn')).toBe('');
    await integrate.shutdown();
    expect(order).toEqual(['start:listening', 'start:quiet', 'stop:quiet', 'stop:listening']);
    expect(logger.text('error')).toContain('Error stopping plugin Quiet: stop failed');
    expect(integrate.lifeCycle.count()).toBe(0);
  });

  it('deregisters a plugin whose start fails', async () => {
    const integrate = new IntegrateTech({ logger: silentLogger });
    class Broken {
      @LifeCycle(ProcessStep.Pending)
      pending() {}
      start() {
        throw new Error('no store');
      }
    }
    const error = await integrate.registerPlugin(new Broken()).catch((caught: unknown) => caught);
    expect(error).toBeInstanceOf(StartupError);
    expect((error as Error).message).toBe('Unable to start plugin Broken: no store');
    expect(integrate.lifeCycle.count()).toBe(0);
    expect(integrate.getPlugins()).toEqual([]);
  });
});
