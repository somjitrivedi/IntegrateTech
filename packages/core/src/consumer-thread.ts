import { sleep } from './concurrency';
import type { Consumer } from './consumer';
import { describeError } from './logger';

export const DEFAULT_BACKOFF_MS: readonly number[] = [3, 5, 8, 13, 21, 34, 55].map((seconds) => seconds * 1000);

export interface ConsumerThreadOptions {
  backoffMs?: readonly number[];
}

export class ConsumerThread {
  private running = false;
  private wake?: AbortController;
  private done?: Promise<void>;
  private attempts = 0;
  private readonly backoffMs: readonly number[];

  constructor(
    readonly consumer: Consumer,
    options: ConsumerThreadOptions = {},
  ) {
    this.backoffMs = options.backoffMs && options.backoffMs.length > 0 ? options.backoffMs : DEFAULT_BACKOFF_MS;
  }

  get name(): string {
    return this.consumer.name;
  }

  isRunning(): boolean {
    return this.running;
  }

  connectAttempts(): number {
    return this.attempts;
  }

  start(): this {
    if (this.done) return this;
    this.running = true;
    this.done = this.run();
    return this;
  }

  private async run(): Promise<void> {
    const logger = this.consumer.integrate.logger;
    const longest = this.backoffMs[this.backoffMs.length - 1]!;
    let attempt = 0;
    do {
      const startedAt = Date.now();
      this.attempts++;
      try {
        await this.consumer.listen();
      } catch (error) {
        if (attempt === 0) logger.error(`Consumer ${this.name} couldn't get connection: ${describeError(error)}`);
        else logger.warn(`Consumer ${this.name} couldn't get connection, attempt ${attempt + 1}: ${describeError(error)}`);
      }
      if (!this.running) break;
      if (Date.now() > startedAt + longest) attempt = 0;
      this.wake = new AbortController();
      await sleep(this.backoffMs[attempt]!, this.wake.signal);
      attempt = Math.min(attempt + 1, this.backoffMs.length - 1);
    } while (this.running);
  }

  shutdown(): void {
    this.running = false;
    this.consumer.shutdown();
    this.wake?.abort();
  }

  async join(timeoutMs?: number): Promise<boolean> {
    if (!this.done) return true;
    if (timeoutMs === undefined) {
      await this.done;
      return true;
    }
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<boolean>((resolve) => {
      timer = setTimeout(() => resolve(false), timeoutMs);
    });
    const finished = await Promise.race([this.done.then(() => true), timeout]);
    clearTimeout(timer);
    return finished;
  }

  async stop(timeoutMs?: number): Promise<boolean> {
    this.shutdown();
    return this.join(timeoutMs);
  }
}
