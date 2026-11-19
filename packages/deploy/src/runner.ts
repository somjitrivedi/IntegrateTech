import {
  ConsumerService,
  type ConsumerThread,
  type HandlerClass,
  type IntegrateTech,
  describeError,
  getConfigFiles,
  hasSourceAnnotation,
  readMeta,
} from '@integratetech/core';

export interface DeploymentContents {
  consumers: HandlerClass[];
  services: Function[];
}

export interface RunnerOptions {
  receiveTimeoutMs?: number;
  backoffMs?: readonly number[];
}

export interface DrainResult {
  drained: boolean;
}

export function inspectClasses(candidates: Iterable<unknown>): DeploymentContents {
  const contents: DeploymentContents = { consumers: [], services: [] };
  const seen = new Set<unknown>();
  for (const candidate of candidates) {
    if (typeof candidate !== 'function' || seen.has(candidate)) continue;
    seen.add(candidate);
    const meta = readMeta(candidate);
    if (!meta) continue;
    if (hasSourceAnnotation(candidate)) contents.consumers.push(candidate as HandlerClass);
    else if (meta.service || meta.coreService) contents.services.push(candidate);
  }
  return contents;
}

export function inspectModule(exports: Record<string, unknown>): DeploymentContents {
  return inspectClasses(Object.values(exports));
}

export function configFilesOf(contents: DeploymentContents): string[] {
  const files = new Set<string>();
  for (const target of [...contents.consumers, ...contents.services]) for (const file of getConfigFiles(target)) files.add(file);
  return [...files].sort();
}

export class DeploymentRunner {
  private threads: ConsumerThread[] = [];
  private services: ConsumerService[] = [];
  private started = false;

  constructor(
    readonly integrate: IntegrateTech,
    readonly contents: DeploymentContents,
    private readonly options: RunnerOptions = {},
  ) {}

  consumerNames(): string[] {
    return this.contents.consumers.map((consumer) => consumer.name);
  }

  serviceNames(): string[] {
    return this.contents.services.map((service) => service.name);
  }

  configFiles(): string[] {
    return configFilesOf(this.contents);
  }

  isStarted(): boolean {
    return this.started;
  }

  processed(): number {
    return this.threads.reduce((total, thread) => total + thread.consumer.processedCount(), 0);
  }

  async start(): Promise<void> {
    if (this.started) return;
    if (this.contents.consumers.length === 0) throw new Error('Deployment contains no consumer classes');
    this.started = true;
    try {
      this.services = this.contents.services.map((service) => new ConsumerService(service as new () => object));
      for (const service of this.services) await service.onDeploy();
      for (const consumer of this.contents.consumers) {
        this.threads.push(
          ...this.integrate.startConsumers(consumer, {
            services: this.services,
            ...(this.options.receiveTimeoutMs !== undefined ? { receiveTimeoutMs: this.options.receiveTimeoutMs } : {}),
            ...(this.options.backoffMs ? { backoffMs: this.options.backoffMs } : {}),
          }),
        );
      }
    } catch (error) {
      await this.drain(0);
      throw error;
    }
  }

  async drain(timeoutMs: number): Promise<DrainResult> {
    if (!this.started) return { drained: true };
    this.started = false;
    const threads = this.threads;
    this.threads = [];
    for (const thread of threads) thread.shutdown();
    const joined = await Promise.all(threads.map((thread) => thread.join(timeoutMs > 0 ? timeoutMs : undefined)));
    for (const service of this.services) {
      try {
        await service.onUndeploy();
      } catch (error) {
        this.integrate.logger.error(`Error undeploying service ${service.serviceClass.name}: ${describeError(error)}`);
      }
    }
    this.services = [];
    return { drained: joined.every(Boolean) };
  }
}
