export class Semaphore {
  private waiters: (() => void)[] = [];
  private inUse = 0;

  constructor(readonly permits: number) {
    if (!Number.isInteger(permits) || permits < 1) throw new Error('Semaphore requires a positive integer');
  }

  get active(): number {
    return this.inUse;
  }

  get waiting(): number {
    return this.waiters.length;
  }

  acquire(): Promise<void> {
    if (this.inUse < this.permits) {
      this.inUse++;
      return Promise.resolve();
    }
    return new Promise((resolve) => this.waiters.push(resolve));
  }

  release(): void {
    const next = this.waiters.shift();
    if (next) next();
    else this.inUse = Math.max(0, this.inUse - 1);
  }
}

export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal?.aborted) return resolve();
    const timer = setTimeout(done, ms);
    function done() {
      clearTimeout(timer);
      signal?.removeEventListener('abort', done);
      resolve();
    }
    signal?.addEventListener('abort', done, { once: true });
  });
}
