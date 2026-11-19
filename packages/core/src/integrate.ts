import type { Connector } from './connector';
import { Consumer, type ConsumerOptions, type HandlerClass } from './consumer';
import { ConsumerThread, type ConsumerThreadOptions } from './consumer-thread';
import { ConnectorError, StartupError } from './errors';
import { LifeCycleMonitors } from './lifecycle';
import { type Logger, consoleLogger, describeError } from './logger';
import { getSources, readMeta } from './metadata';
import { PropertiesRegistry } from './properties';

export interface IntegratePlugin {
  start?(integrate: IntegrateTech): void | Promise<void>;
  stop?(): void | Promise<void>;
}

export interface IntegrateTechOptions {
  logger?: Logger;
  properties?: PropertiesRegistry;
}

export class IntegrateTech {
  readonly logger: Logger;
  readonly lifeCycle: LifeCycleMonitors;
  readonly properties: PropertiesRegistry;
  private connector?: Connector;
  private running = false;
  private readonly plugins: object[] = [];

  constructor(options: IntegrateTechOptions = {}) {
    this.logger = options.logger ?? consoleLogger;
    this.lifeCycle = new LifeCycleMonitors(this.logger);
    this.properties = options.properties ?? new PropertiesRegistry();
  }

  async start(connector: Connector): Promise<void> {
    this.connector = connector;
    try {
      await connector.start();
    } catch (error) {
      throw new StartupError(describeError(error), { cause: error });
    }
    this.running = true;
  }

  async registerPlugin(plugin: object): Promise<void> {
    if (this.plugins.includes(plugin)) return;
    const owner = (plugin as { constructor: Function }).constructor;
    const listens = (readMeta(owner)?.lifecycle.length ?? 0) > 0;
    if (listens) this.lifeCycle.register(plugin);
    try {
      await (plugin as IntegratePlugin).start?.(this);
    } catch (error) {
      if (listens) this.lifeCycle.deregister(owner);
      throw new StartupError(`Unable to start plugin ${owner.name}: ${describeError(error)}`, { cause: error });
    }
    this.plugins.push(plugin);
  }

  getPlugins(): readonly object[] {
    return this.plugins;
  }

  async stopPlugins(): Promise<void> {
    while (this.plugins.length > 0) {
      const plugin = this.plugins.pop()!;
      const owner = (plugin as { constructor: Function }).constructor;
      this.lifeCycle.deregister(owner);
      try {
        await (plugin as IntegratePlugin).stop?.();
      } catch (error) {
        this.logger.error(`Error stopping plugin ${owner.name}: ${describeError(error)}`);
      }
    }
  }

  async shutdown(): Promise<void> {
    await this.stopPlugins();
    if (!this.running) return;
    this.running = false;
    try {
      await this.connector?.stop();
    } catch (error) {
      this.logger.error(`Error stopping connector: ${describeError(error)}`);
    }
  }

  isRunning(): boolean {
    return this.running;
  }

  getConnector(): Connector {
    if (!this.connector) throw new ConnectorError('IntegrateTech has no connector; call start() first');
    return this.connector;
  }

  setConnector(connector: Connector): void {
    this.connector = connector;
  }

  getLifeCycle(): LifeCycleMonitors {
    return this.lifeCycle;
  }

  createConsumers(handlerClass: HandlerClass, options: Omit<ConsumerOptions, 'source' | 'roleSources'> = {}): Consumer[] {
    const sources = getSources(handlerClass);
    if (sources.length === 0) throw new Error('A consumer must consume at least one source');
    return sources.map((source) => new Consumer(handlerClass, this, { ...options, source, roleSources: sources }));
  }

  startConsumers(
    handlerClass: HandlerClass,
    options: Omit<ConsumerOptions, 'source' | 'roleSources'> & ConsumerThreadOptions = {},
  ): ConsumerThread[] {
    return this.createConsumers(handlerClass, options).map((consumer) => new ConsumerThread(consumer, options).start());
  }
}
