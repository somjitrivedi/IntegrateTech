import { RESPONSE_URI, parseResponseUri } from './message-runnable';
import type { MessageProducer, SendBody, SendExtras } from './producer';

export class ResponseTimeoutError extends Error {
  constructor(readonly correlationId: string, timeoutMs: number) {
    super(`No response for ${correlationId} within ${timeoutMs}ms`);
    this.name = 'ResponseTimeoutError';
  }
}

interface PendingResult<T> {
  resolve: (value: T) => void;
  reject: (error: unknown) => void;
  timer?: ReturnType<typeof setTimeout>;
}

export class ResultResolver<T> {
  private readonly pending = new Map<string, PendingResult<T>>();

  register(correlationId: string, timeoutMs?: number): Promise<T> {
    if (this.pending.has(correlationId)) throw new Error('Duplicate correlation id, unable to register future.');
    return new Promise<T>((resolve, reject) => {
      const entry: PendingResult<T> = { resolve, reject };
      if (timeoutMs !== undefined && timeoutMs > 0) {
        entry.timer = setTimeout(() => {
          this.pending.delete(correlationId);
          reject(new ResponseTimeoutError(correlationId, timeoutMs));
        }, timeoutMs);
      }
      this.pending.set(correlationId, entry);
    });
  }

  resolve(correlationId: string | undefined, value: T): boolean {
    if (correlationId === undefined) return false;
    const entry = this.pending.get(correlationId);
    if (!entry) return false;
    this.pending.delete(correlationId);
    clearTimeout(entry.timer);
    entry.resolve(value);
    return true;
  }

  reject(correlationId: string, error: unknown): boolean {
    const entry = this.pending.get(correlationId);
    if (!entry) return false;
    this.pending.delete(correlationId);
    clearTimeout(entry.timer);
    entry.reject(error);
    return true;
  }

  isPending(correlationId: string): boolean {
    return this.pending.has(correlationId);
  }

  pendingCount(): number {
    return this.pending.size;
  }
}

export interface SyncProducerOptions {
  timeoutMs?: number;
}

export interface SyncSend<T> {
  correlationId: string;
  result: Promise<T>;
}

export class SyncProducer<T> {
  private readonly timeoutMs: number | undefined;

  constructor(
    private readonly producer: MessageProducer,
    private readonly resolver: ResultResolver<T>,
    readonly uri: string,
    options: SyncProducerOptions = {},
  ) {
    if (!parseResponseUri(uri)) throw new Error(`Invalid response uri: ${uri}`);
    this.timeoutMs = options.timeoutMs;
    producer.setProperties({ ...producer.getProperties(), [RESPONSE_URI]: uri });
  }

  async send(body: SendBody, extras?: SendExtras): Promise<SyncSend<T>> {
    const correlationId = await this.producer.send(body, extras);
    return { correlationId, result: this.resolver.register(correlationId, this.timeoutMs) };
  }

  async sendMap(map: Record<string, string> | Map<string, string>, extras?: SendExtras): Promise<SyncSend<T>> {
    const correlationId = await this.producer.sendMap(map, extras);
    return { correlationId, result: this.resolver.register(correlationId, this.timeoutMs) };
  }

  async request(body: SendBody, extras?: SendExtras): Promise<T> {
    return (await this.send(body, extras)).result;
  }

  close(): Promise<void> {
    return this.producer.close();
  }
}
