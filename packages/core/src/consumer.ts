import { Semaphore } from './concurrency';
import type { MessageConsumer } from './connector';
import { ConsumerService } from './consumer-service';
import type { IntegrateTech } from './integrate';
import { parseKeyValue } from './key-value';
import { describeError } from './logger';
import type { IntegrateMessage } from './message';
import { type Closeable, MessageRunnable } from './message-runnable';
import { type ClassMeta, type InjectionSpec, type MemberKey, getSources, readMeta } from './metadata';
import type { ProcessStep } from './process-step';
import type { MessageProducer } from './producer';
import type { Source } from './sources';

export type HandlerClass = new (...args: any[]) => object;

export interface ConsumerOptions {
  source?: Source;
  roleSources?: Source[];
  services?: (ConsumerService | object)[];
  receiveTimeoutMs?: number;
}

export type OutOfMessagesHandler = (consumer: Consumer) => void | Promise<void>;

let nextConsumerId = 1;

class InjectionFailure extends Error {}

function toService(service: ConsumerService | object): ConsumerService {
  return service instanceof ConsumerService ? service : new ConsumerService(service);
}

function isProducer(value: unknown): value is MessageProducer {
  return typeof value === 'object' && value !== null && typeof (value as MessageProducer).send === 'function' && typeof (value as MessageProducer).close === 'function';
}

export class Consumer {
  readonly name: string;
  readonly source: Source;
  readonly roleSources: Source[];
  readonly meta: ClassMeta;
  readonly receiveTimeoutMs: number;
  private services: ConsumerService[];
  private running = false;
  private abort?: AbortController;
  private outOfMessages?: OutOfMessagesHandler;
  private readonly orderChains = new Map<string, Promise<unknown>>();
  private readonly inflight = new Set<Promise<unknown>>();
  private processed = 0;

  constructor(
    readonly handlerClass: HandlerClass,
    readonly integrate: IntegrateTech,
    options: ConsumerOptions = {},
  ) {
    const meta = readMeta(handlerClass);
    if (!meta) throw new Error(`${handlerClass.name} is not a consumer: no decorators found`);
    this.meta = meta;
    if (options.source) {
      this.source = options.source.contextClass ? options.source : options.source.setContextClass(handlerClass);
      this.roleSources = options.roleSources ?? [this.source];
    } else {
      const sources = getSources(handlerClass);
      if (sources.length > 1) throw new Error('One consumer instance cannot consume more than one source');
      if (sources.length === 0) throw new Error('A consumer must consume at least one source');
      this.source = sources[0]!;
      this.roleSources = options.roleSources ?? sources;
    }
    const connector = integrate.getConnector();
    if (meta.onResponse.length > 0 && !connector.supportsResponse()) throw new Error('@Response is not supported by the current connector');
    if (meta.order !== undefined && !connector.supportsOrder()) throw new Error('@Order is not supported by the current connector');
    this.services = (options.services ?? []).map(toService);
    this.receiveTimeoutMs = options.receiveTimeoutMs ?? 2500;
    this.name = `${this.source.label()}:${nextConsumerId++}`;
  }

  getHandlerClass(): HandlerClass {
    return this.handlerClass;
  }

  getSource(): Source {
    return this.source;
  }

  getRoleSources<S extends Source>(kind?: S['kind']): S[] {
    return (kind ? this.roleSources.filter((source) => source.kind === kind) : this.roleSources) as S[];
  }

  addServices(...services: (ConsumerService | object)[]): void {
    this.services.push(...services.map(toService));
  }

  setServices(services: (ConsumerService | object)[]): void {
    this.services = services.map(toService);
  }

  setOutOfMessages(handler: OutOfMessagesHandler | undefined): void {
    this.outOfMessages = handler;
  }

  isRunning(): boolean {
    return this.running;
  }

  processedCount(): number {
    return this.processed;
  }

  hooksFor(step: ProcessStep): MemberKey[] {
    return this.meta.hooks.get(step) ?? [];
  }

  shutdown(): void {
    this.running = false;
    this.abort?.abort();
  }

  async listen(): Promise<void> {
    const consumer = await this.integrate.getConnector().getConsumerForSource(this.source);
    try {
      await this.messageLoop(consumer);
    } finally {
      await consumer.close();
    }
  }

  async messageLoop(consumer: MessageConsumer): Promise<void> {
    this.running = true;
    this.abort = new AbortController();
    const signal = this.abort.signal;
    const threads = this.meta.multiThreaded;
    const workers = new Semaphore(threads ?? 1);
    const capacity = new Semaphore(threads ? threads * 100 + 100 : 1);
    try {
      while (this.running) {
        await capacity.acquire();
        let msg: IntegrateMessage | null;
        try {
          msg = await consumer.receive(this.receiveTimeoutMs, signal);
        } catch (error) {
          capacity.release();
          throw error;
        }
        if (!msg) {
          capacity.release();
          if (this.running && this.outOfMessages) await this.outOfMessages(this);
          continue;
        }
        if (!this.running) {
          capacity.release();
          break;
        }
        let task: Promise<unknown>;
        try {
          const runner = await this.prepare(msg);
          const orderId = this.meta.order !== undefined ? String(await runner.handler[this.meta.order]()) : undefined;
          task = this.schedule(runner, workers, orderId);
        } catch (error) {
          capacity.release();
          this.integrate.logger.error(`Consumer ${this.name} couldn't be used: ${describeError(error)}`);
          this.running = false;
          break;
        }
        const tracked = task.then(
          () => undefined,
          () => undefined,
        );
        this.inflight.add(tracked);
        void tracked.then(() => {
          this.inflight.delete(tracked);
          capacity.release();
        });
        if (!threads) await tracked;
      }
    } finally {
      this.running = false;
      await Promise.all([...this.inflight]);
    }
  }

  private schedule(runner: MessageRunnable, workers: Semaphore, orderId: string | undefined): Promise<unknown> {
    const execute = async () => {
      await workers.acquire();
      try {
        await runner.run();
        this.processed++;
      } finally {
        workers.release();
      }
    };
    if (orderId === undefined) return execute();
    const previous = this.orderChains.get(orderId) ?? Promise.resolve();
    const chained = previous.then(execute, execute);
    this.orderChains.set(orderId, chained);
    const cleanup = () => {
      if (this.orderChains.get(orderId) === chained) this.orderChains.delete(orderId);
    };
    chained.then(cleanup, cleanup);
    return chained;
  }

  async prepare(msg: IntegrateMessage): Promise<MessageRunnable> {
    const closeMe: Closeable[] = [];
    try {
      const handler = await this.construct(msg, closeMe);
      await this.inject(msg, handler, closeMe);
      const runner = new MessageRunnable(this, msg, handler, closeMe);
      await runner.pending();
      return runner;
    } catch (error) {
      for (const closeable of closeMe) await Promise.resolve(closeable.close()).catch(() => undefined);
      throw error;
    }
  }

  async getHandlerInstance(msg: IntegrateMessage): Promise<object> {
    const closeMe: Closeable[] = [];
    const handler = await this.construct(msg, closeMe);
    await this.inject(msg, handler, closeMe);
    return handler;
  }

  private async construct(msg: IntegrateMessage, closeMe: Closeable[]): Promise<any> {
    if (this.meta.params.size === 0) return new this.handlerClass();
    const length = Math.max(...this.meta.params.keys()) + 1;
    const args: unknown[] = [];
    for (let index = 0; index < length; index++) {
      const spec = this.meta.params.get(index);
      args.push(spec ? await this.resolveValue(spec, undefined, msg, closeMe) : undefined);
    }
    return new this.handlerClass(...args);
  }

  async inject(msg: IntegrateMessage, handler: any, closeMe: Closeable[] = []): Promise<void> {
    for (const field of this.meta.fields) {
      const value = await this.resolveValue(field.spec, typeof field.key === 'string' ? field.key : undefined, msg, closeMe);
      if (value !== undefined) handler[field.key] = value;
    }
  }

  private parseJson(body: string): unknown {
    try {
      return JSON.parse(body);
    } catch (error) {
      this.integrate.logger.warn(`Unable to parse json for injection: ${describeError(error)}`);
      return undefined;
    }
  }

  private async resolveValue(spec: InjectionSpec, fieldName: string | undefined, msg: IntegrateMessage, closeMe: Closeable[]): Promise<unknown> {
    try {
      switch (spec.kind) {
        case 'inject': {
          for (const service of this.services) {
            try {
              const value = await service.resolve(spec.type, spec.name ?? fieldName);
              if (value !== undefined) return value;
            } catch {
              continue;
            }
          }
          return undefined;
        }
        case 'message': {
          if (spec.as === 'message') return msg;
          if (msg.body === undefined) return undefined;
          if (spec.as === 'string') return msg.body;
          if (spec.as === 'map') return msg.body.trimStart().startsWith('{') ? this.parseJson(msg.body) : parseKeyValue(msg.body);
          const parsed = this.parseJson(msg.body);
          if (spec.as === 'json' || parsed === undefined) return parsed;
          return Object.assign(new spec.as(), parsed);
        }
        case 'headers':
          return spec.header ? msg.headers?.[spec.header] : msg.headers;
        case 'properties':
          return spec.name !== undefined ? msg.properties?.[spec.name] : msg.properties;
        case 'config': {
          const file = spec.file ?? fieldName ?? '';
          const config = this.integrate.properties.get(file);
          if (!config) {
            this.integrate.logger.warn(`Attempt to inject field failed because resource file ${file} was not found`);
            return undefined;
          }
          return spec.field ? config[spec.field] : config;
        }
        case 'producer': {
          const connector = this.integrate.getConnector();
          const producer = spec.queue ? connector.getQueueProducer(spec.queue) : spec.topic ? connector.getTopicProducer(spec.topic) : undefined;
          if (isProducer(producer)) closeMe.push(producer);
          return producer;
        }
      }
    } catch (error) {
      if (error instanceof InjectionFailure) throw error;
      throw new InjectionFailure(`Error injecting data into ${this.handlerClass.name}: ${describeError(error)}`, { cause: error });
    }
  }
}
