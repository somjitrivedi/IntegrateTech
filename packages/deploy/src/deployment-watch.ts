import { type Logger, describeError, silentLogger, sleep } from '@integratetech/core';
import type { FileScanner, ScanResult } from './file-scanner';

export const DEFAULT_POLL_INTERVAL_MS = 10_000;
export const DEFAULT_SETTLE_MS = 1_000;

export type WatchHandler = (results: ScanResult[]) => void | Promise<void>;

export interface DeploymentWatchOptions {
  intervalMs?: number;
  settleMs?: number;
  logger?: Logger;
}

export class DeploymentWatch {
  readonly intervalMs: number;
  readonly settleMs: number;
  private readonly logger: Logger;
  private timer?: ReturnType<typeof setTimeout>;
  private running = false;
  private current?: Promise<void>;
  private settle?: AbortController;
  private scans = 0;

  constructor(
    private readonly scanners: FileScanner[],
    private readonly handler: WatchHandler,
    options: DeploymentWatchOptions = {},
  ) {
    this.intervalMs = Math.max(1, options.intervalMs ?? DEFAULT_POLL_INTERVAL_MS);
    this.settleMs = Math.max(0, options.settleMs ?? DEFAULT_SETTLE_MS);
    this.logger = options.logger ?? silentLogger;
  }

  isRunning(): boolean {
    return this.running;
  }

  scanCount(): number {
    return this.scans;
  }

  async start(): Promise<void> {
    if (this.running) return;
    this.running = true;
    await this.scanNow();
    if (!this.running) return;
    this.settle = new AbortController();
    await sleep(this.settleMs, this.settle.signal);
    this.settle = undefined;
    if (!this.running) return;
    await this.scanNow();
    this.schedule();
  }

  scanNow(): Promise<void> {
    if (this.current) return this.current;
    this.current = this.tick().finally(() => {
      this.current = undefined;
    });
    return this.current;
  }

  private async tick(): Promise<void> {
    this.scans++;
    try {
      const results: ScanResult[] = [];
      for (const scanner of this.scanners) results.push(await scanner.scan());
      await this.handler(results);
    } catch (error) {
      this.logger.error(`Deployment scan failed: ${describeError(error)}`);
    }
  }

  private schedule(): void {
    if (!this.running) return;
    this.timer = setTimeout(() => {
      void this.scanNow().then(() => this.schedule());
    }, this.intervalMs);
  }

  async stop(): Promise<void> {
    this.running = false;
    this.settle?.abort();
    clearTimeout(this.timer);
    this.timer = undefined;
    await this.current;
  }
}
