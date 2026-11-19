import { randomUUID } from 'node:crypto';
import { ProducerError } from './errors';
import { Header, type HeaderMap, sanitizeHeaders } from './headers';
import { formatKeyValue } from './key-value';
import { IntegrateMessage } from './message';

export interface SendExtras {
  headers?: HeaderMap;
  properties?: Record<string, string>;
  delayMs?: number;
}

export type SendBody = string | IntegrateMessage | object;

export interface MessageProducer {
  send(body: SendBody, extras?: SendExtras): Promise<string>;
  sendMap(map: Record<string, string> | Map<string, string>, extras?: SendExtras): Promise<string>;
  getHeaders(): HeaderMap;
  setHeaders(headers: HeaderMap): void;
  getProperties(): Record<string, string>;
  setProperties(properties: Record<string, string>): void;
  close(): Promise<void>;
}

export interface OutboundMessage {
  id: string;
  body: string;
  headers: HeaderMap;
  properties: Record<string, string>;
  delayMs: number;
  priority: number;
  expiresAt: number | null;
  createdAt: number;
}

export const DEFAULT_PRIORITY = 4;

export function newMessageId(): string {
  return randomUUID().replace(/-/g, '');
}

function nonEmpty(value: unknown): string | undefined {
  if (value === undefined || value === null) return undefined;
  const text = String(value).trim();
  return text === '' ? undefined : text;
}

function serialize(body: object): string {
  try {
    const text = JSON.stringify(body);
    if (text === undefined) throw new Error('Body is not serializable');
    return text;
  } catch (error) {
    throw new ProducerError('Failed to serialize message body', { cause: error });
  }
}

export function toMessage(body: SendBody): IntegrateMessage {
  if (body instanceof IntegrateMessage) return body;
  if (typeof body === 'string') return new IntegrateMessage(body);
  if (body === null || body === undefined) throw new ProducerError('Message body is required');
  return new IntegrateMessage(serialize(body));
}

export abstract class AbstractProducer implements MessageProducer {
  protected headers: HeaderMap = {};
  protected properties: Record<string, string> = {};
  private closed = false;

  async send(body: SendBody, extras: SendExtras = {}): Promise<string> {
    if (this.closed) throw new ProducerError('Producer is closed');
    const message = toMessage(body);
    if (message.body === undefined) throw new ProducerError('Message body is required');
    const headers: HeaderMap = sanitizeHeaders({ ...this.headers, ...message.headers, ...extras.headers });
    const properties = { ...this.properties, ...message.properties, ...extras.properties };
    const id = nonEmpty(message.id) ?? nonEmpty(headers[Header.CorrelationId]) ?? newMessageId();
    const createdAt = Date.now();
    headers[Header.CorrelationId] = id;
    headers[Header.Timestamp] ??= createdAt;
    const priority = Math.min(9, Math.max(0, Math.trunc(Number(headers[Header.Priority] ?? DEFAULT_PRIORITY))));
    const expiration = Number(headers[Header.Expiration] ?? 0);
    const outbound: OutboundMessage = {
      id,
      body: message.body,
      headers,
      properties,
      delayMs: Math.max(0, extras.delayMs ?? 0),
      priority: Number.isFinite(priority) ? priority : DEFAULT_PRIORITY,
      expiresAt: expiration > 0 ? expiration : null,
      createdAt,
    };
    try {
      await this.publish(outbound);
    } catch (error) {
      throw new ProducerError('Failed to send message', { cause: error });
    }
    return id;
  }

  sendMap(map: Record<string, string> | Map<string, string>, extras?: SendExtras): Promise<string> {
    return this.send(new IntegrateMessage(formatKeyValue(map)), extras);
  }

  getHeaders(): HeaderMap {
    return this.headers;
  }

  setHeaders(headers: HeaderMap): void {
    this.headers = sanitizeHeaders(headers);
  }

  getProperties(): Record<string, string> {
    return this.properties;
  }

  setProperties(properties: Record<string, string>): void {
    this.properties = { ...properties };
  }

  isClosed(): boolean {
    return this.closed;
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    await this.onClose();
  }

  protected abstract publish(message: OutboundMessage): Promise<void>;

  protected async onClose(): Promise<void> {}
}
