import type { MessageConsumer } from './connector';
import { type HeaderMap, sanitizeHeaders } from './headers';
import { type Logger, describeError, silentLogger } from './logger';
import { IntegrateMessage } from './message';

class AcknowledgingMessage extends IntegrateMessage {
  constructor(private readonly source: IntegrateMessage | undefined) {
    super();
  }

  override acknowledge(): Promise<boolean> {
    return this.source ? this.source.acknowledge() : Promise.resolve(true);
  }

  override beforeProcessing(): Promise<boolean> {
    return this.source ? this.source.beforeProcessing() : Promise.resolve(true);
  }
}

export class RoleInputMessage {
  readonly properties?: Record<string, string>;
  readonly headers?: HeaderMap;

  constructor(
    readonly role: string | undefined,
    readonly id: string | undefined,
    readonly msg: string | undefined,
    properties?: Record<string, string> | null,
    headers?: HeaderMap | null,
  ) {
    if (properties) this.properties = { ...properties };
    if (headers) this.headers = sanitizeHeaders(headers);
  }

  static fromString(input: string | undefined): RoleInputMessage {
    try {
      const parsed: unknown = JSON.parse(input ?? '');
      if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) throw new Error('not an object');
      const value = parsed as Record<string, unknown>;
      const text = (field: unknown) => (typeof field === 'string' ? field : undefined);
      const properties = typeof value.properties === 'object' && value.properties !== null ? (value.properties as Record<string, unknown>) : null;
      const stringProperties = properties
        ? Object.fromEntries(Object.entries(properties).filter(([, v]) => v !== null && v !== undefined).map(([k, v]) => [k, String(v)]))
        : null;
      const headers = typeof value.headers === 'object' && value.headers !== null ? (value.headers as Record<string, unknown>) : null;
      return new RoleInputMessage(text(value.role), text(value.id), text(value.msg), stringProperties, headers ? sanitizeHeaders(headers) : null);
    } catch {
      throw new Error(`Could not deserialize RoleInputMessage from JSON: ${input}`);
    }
  }

  static fromMessage(role: string, input: IntegrateMessage): RoleInputMessage {
    return new RoleInputMessage(role, input.id, input.body, input.properties, input.headers);
  }

  toString(): string {
    return JSON.stringify({ role: this.role, id: this.id, msg: this.msg, properties: this.properties ?? null, headers: this.headers ?? null });
  }

  toMessage(source?: IntegrateMessage): IntegrateMessage {
    const result = new AcknowledgingMessage(source);
    if (this.id !== undefined) result.id = this.id;
    if (this.msg !== undefined) result.body = this.msg;
    if (this.properties) result.setProperties(this.properties);
    if (this.headers) result.setHeaders(this.headers);
    return result;
  }

  equals(other: unknown): boolean {
    return other instanceof RoleInputMessage && other.toString() === this.toString();
  }
}

export class RoleInputConsumer implements MessageConsumer {
  constructor(
    private readonly inner: MessageConsumer,
    private readonly logger: Logger = silentLogger,
  ) {}

  async receive(timeoutMs: number, signal?: AbortSignal): Promise<IntegrateMessage | null> {
    const deadline = Date.now() + timeoutMs;
    while (true) {
      const initial = await this.inner.receive(Math.max(0, deadline - Date.now()), signal);
      if (!initial) return null;
      try {
        return RoleInputMessage.fromString(initial.body).toMessage(initial);
      } catch (error) {
        this.logger.error(`Dropping malformed role input message ${initial.id}: ${describeError(error)}`);
        await initial.acknowledge();
        if (Date.now() >= deadline || signal?.aborted) return null;
      }
    }
  }

  close(): Promise<void> {
    return this.inner.close();
  }
}
