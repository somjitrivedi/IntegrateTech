import { basename } from 'node:path';
import { Worker } from 'node:worker_threads';
import { type Logger, describeError, silentLogger } from '@integratetech/core';
import { DeploymentBase, DeploymentError } from './deployment';
import type { PropertiesSnapshot } from './properties-manager';
import type { MainToWorker, ModuleSpec, WorkerInit, WorkerToMain } from './protocol';
import type { DrainResult, RunnerOptions } from './runner';

export const DEFAULT_START_TIMEOUT_MS = 30_000;
export const DEFAULT_DRAIN_TIMEOUT_MS = 30_000;
const EXIT_GRACE_MS = 2_000;

export interface WorkerDeploymentOptions {
  connector: ModuleSpec;
  plugins?: ModuleSpec[];
  runner?: RunnerOptions;
  startTimeoutMs?: number;
  drainTimeoutMs?: number;
  logger?: Logger;
  bootstrap?: URL | string;
  onFailure?: (deployment: WorkerDeployment, error: string) => void;
}

type Message<T extends WorkerToMain['type']> = Extract<WorkerToMain, { type: T }>;

interface Waiter {
  types: WorkerToMain['type'][];
  failOnError: boolean;
  resolve: (message: WorkerToMain) => void;
  reject: (error: Error) => void;
}

function timeout(ms: number): Promise<'timeout'> {
  return new Promise((resolve) => setTimeout(() => resolve('timeout'), ms).unref?.());
}

export class WorkerDeployment extends DeploymentBase {
  readonly kind = 'file' as const;
  private worker?: Worker;
  private exited?: Promise<number>;
  private alive = false;
  private lastError?: string;
  private waiters = new Set<Waiter>();
  private readonly logger: Logger;
  private readonly startTimeoutMs: number;
  private readonly drainTimeoutMs: number;
  private exits = 0;

  constructor(
    readonly path: string,
    private readonly options: WorkerDeploymentOptions,
    name = basename(path),
  ) {
    super(name);
    this.logger = options.logger ?? silentLogger;
    this.startTimeoutMs = options.startTimeoutMs ?? DEFAULT_START_TIMEOUT_MS;
    this.drainTimeoutMs = options.drainTimeoutMs ?? DEFAULT_DRAIN_TIMEOUT_MS;
  }

  protected override file(): string {
    return this.path;
  }

  isAlive(): boolean {
    return this.alive;
  }

  threadId(): number | undefined {
    return this.alive ? this.worker?.threadId : undefined;
  }

  exitCount(): number {
    return this.exits;
  }

  private onMessage(message: WorkerToMain): void {
    if (message.type === 'log') {
      this.logger[message.level](`[${this.name}] ${message.message}`);
      return;
    }
    if (message.type === 'error') {
      this.lastError = message.message;
      this.logger.error(`[${this.name}] ${message.phase} failed: ${message.message}`);
      for (const waiter of [...this.waiters]) {
        if (!waiter.failOnError) continue;
        this.waiters.delete(waiter);
        waiter.reject(new DeploymentError(message.message));
      }
      return;
    }
    for (const waiter of [...this.waiters]) {
      if (!waiter.types.includes(message.type)) continue;
      this.waiters.delete(waiter);
      waiter.resolve(message);
    }
  }

  private onExit(code: number): void {
    this.alive = false;
    this.exits++;
    const reason = this.lastError ?? `worker exited with code ${code}`;
    for (const waiter of [...this.waiters]) waiter.reject(new DeploymentError(reason));
    this.waiters.clear();
    if (this.status === 'running') {
      this.status = 'failed';
      this.error = reason;
      this.logger.error(`[${this.name}] worker stopped unexpectedly: ${reason}`);
      this.options.onFailure?.(this, reason);
    }
  }

  private next<T extends WorkerToMain['type']>(types: T[], timeoutMs: number, failOnError: boolean): Promise<Message<T>> {
    return new Promise((resolve, reject) => {
      if (!this.alive) {
        reject(new DeploymentError(this.lastError ?? 'worker is not running'));
        return;
      }
      const timer = setTimeout(() => {
        this.waiters.delete(waiter);
        reject(new DeploymentError(`Timed out after ${timeoutMs}ms waiting for ${types.join(' or ')}`));
      }, timeoutMs);
      const waiter: Waiter = {
        types,
        failOnError,
        resolve: (message) => {
          clearTimeout(timer);
          resolve(message as Message<T>);
        },
        reject: (error) => {
          clearTimeout(timer);
          reject(error);
        },
      };
      this.waiters.add(waiter);
    });
  }

  private post(message: MainToWorker): void {
    this.worker?.postMessage(message);
  }

  async start(properties: PropertiesSnapshot): Promise<void> {
    if (this.alive) throw new DeploymentError(`Deployment ${this.name} is already running`);
    this.status = 'starting';
    this.generation++;
    this.error = undefined;
    this.lastError = undefined;
    const init: WorkerInit = {
      name: this.name,
      bundle: this.path,
      connector: this.options.connector,
      plugins: this.options.plugins ?? [],
      properties,
      runner: this.options.runner ?? {},
    };
    const bootstrap = this.options.bootstrap ?? new URL('./worker-bootstrap.ts', import.meta.url);
    try {
      const worker = new Worker(bootstrap, { workerData: init, name: `integratetech:${this.name}` });
      this.worker = worker;
      this.alive = true;
      this.exited = new Promise((resolve) => {
        worker.once('exit', (code) => {
          this.onExit(code);
          resolve(code);
        });
      });
      worker.on('message', (message: WorkerToMain) => this.onMessage(message));
      worker.on('error', (error) => {
        this.lastError = describeError(error);
      });
      const ready = await this.next(['ready'], this.startTimeoutMs, true);
      this.consumers = ready.consumers;
      this.services = ready.services;
      this.files = ready.configFiles;
      this.post({ type: 'start' });
      await this.next(['started'], this.startTimeoutMs, true);
    } catch (error) {
      await this.kill();
      this.status = 'failed';
      this.error = describeError(error);
      throw new DeploymentError(`Unable to start deployment ${this.name}: ${this.error}`, { cause: error });
    }
    this.startedAt = Date.now();
    this.status = 'running';
  }

  private async kill(): Promise<void> {
    const worker = this.worker;
    if (!worker) return;
    if (this.alive) await worker.terminate();
    await this.exited;
    this.worker = undefined;
  }

  async stop(): Promise<DrainResult> {
    if (!this.worker || !this.alive) {
      this.worker = undefined;
      if (this.status !== 'failed') this.status = 'stopped';
      return { drained: true };
    }
    this.status = 'stopping';
    let drained = false;
    try {
      const stopped = this.next(['stopped'], this.drainTimeoutMs + EXIT_GRACE_MS, false);
      this.post({ type: 'drain', timeoutMs: this.drainTimeoutMs });
      drained = (await stopped).drained;
    } catch (error) {
      this.logger.warn(`[${this.name}] drain did not finish: ${describeError(error)}`);
    }
    if (this.alive && (await Promise.race([this.exited!, timeout(EXIT_GRACE_MS)])) === 'timeout') {
      this.logger.debug(`[${this.name}] terminating worker`);
    }
    if (!drained) this.logger.warn(`[${this.name}] force-terminated before in-flight messages finished`);
    await this.kill();
    this.status = 'stopped';
    return { drained };
  }
}
