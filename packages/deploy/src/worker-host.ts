import { pathToFileURL } from 'node:url';
import { type MessagePort, parentPort, workerData } from 'node:worker_threads';
import { type Connector, IntegrateTech, type Logger, PropertiesRegistry, describeError } from '@integratetech/core';
import { type LogLevel, type MainToWorker, type WorkerInit, type WorkerToMain, instantiate } from './protocol';
import { DeploymentRunner, inspectModule } from './runner';

function portLogger(port: MessagePort): Logger {
  const send = (level: LogLevel) => (message: string, ...args: unknown[]) => {
    const text = args.length > 0 ? `${message} ${args.map((arg) => describeError(arg)).join(' ')}` : message;
    port.postMessage({ type: 'log', level, message: text } satisfies WorkerToMain);
  };
  return { debug: send('debug'), info: send('info'), warn: send('warn'), error: send('error') };
}

export class WorkerHost {
  private integrate?: IntegrateTech;
  private runner?: DeploymentRunner;
  private finished = false;
  private readonly logger: Logger;

  constructor(
    private readonly port: MessagePort,
    private readonly init: WorkerInit,
  ) {
    this.logger = portLogger(port);
  }

  private post(message: WorkerToMain): void {
    this.port.postMessage(message);
  }

  async load(): Promise<boolean> {
    try {
      const properties = new PropertiesRegistry();
      for (const [name, values] of Object.entries(this.init.properties)) properties.set(name, values);
      const integrate = new IntegrateTech({ logger: this.logger, properties });
      this.integrate = integrate;
      await integrate.start(await instantiate<Connector>(this.init.connector));
      for (const spec of this.init.plugins) await integrate.registerPlugin(await instantiate<object>(spec));
      const exports = (await import(pathToFileURL(this.init.bundle).href)) as Record<string, unknown>;
      const contents = inspectModule(exports);
      if (contents.consumers.length === 0) throw new Error(`Bundle ${this.init.name} exports no consumer classes`);
      this.runner = new DeploymentRunner(integrate, contents, this.init.runner);
      this.post({ type: 'ready', consumers: this.runner.consumerNames(), services: this.runner.serviceNames(), configFiles: this.runner.configFiles() });
      return true;
    } catch (error) {
      await this.shutdown();
      this.post({ type: 'error', phase: 'load', message: describeError(error) });
      this.port.close();
      return false;
    }
  }

  async handle(message: MainToWorker): Promise<void> {
    if (message.type === 'start') {
      try {
        await this.runner!.start();
        this.post({ type: 'started' });
      } catch (error) {
        await this.runner?.drain(0);
        await this.shutdown();
        this.post({ type: 'error', phase: 'start', message: describeError(error) });
        this.port.close();
      }
      return;
    }
    if (message.type === 'drain') {
      let drained = true;
      try {
        drained = (await this.runner?.drain(message.timeoutMs))?.drained ?? true;
      } catch (error) {
        drained = false;
        this.post({ type: 'error', phase: 'drain', message: describeError(error) });
      }
      await this.shutdown();
      this.post({ type: 'stopped', drained });
      this.port.close();
    }
  }

  private async shutdown(): Promise<void> {
    if (this.finished) return;
    this.finished = true;
    try {
      await this.integrate?.shutdown();
    } catch (error) {
      this.logger.error(`Error shutting down deployment ${this.init.name}: ${describeError(error)}`);
    }
  }
}

if (parentPort && (workerData as WorkerInit | undefined)?.bundle) {
  const port = parentPort;
  const host = new WorkerHost(port, workerData as WorkerInit);
  if (await host.load()) port.on('message', (message: MainToWorker) => void host.handle(message));
}
