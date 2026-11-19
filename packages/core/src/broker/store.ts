import type { HeaderMap } from '../headers';

export interface StoredMessage {
  id: string;
  body: string;
  headers: HeaderMap;
  properties: Record<string, string>;
  createdAt: number;
  expiresAt: number | null;
}

export interface InsertOptions {
  availableAt: number;
  priority: number;
}

export interface LeaseRequest {
  channel: string;
  owner: string;
  token: string;
  now: number;
  leaseMs: number;
  maxAttempts: number;
}

export interface LeasedDelivery {
  seq: number;
  channel: string;
  attempts: number;
  message: StoredMessage;
}

export interface DeadLetter {
  seq: number;
  channel: string;
  attempts: number;
  reason: string;
  deadAt: number;
  message: StoredMessage;
}

export interface BrokerStore {
  init(): Promise<void>;
  close(): Promise<void>;
  insert(message: StoredMessage, channels: string[], options: InsertOptions): Promise<void>;
  subscribe(topic: string, channel: string, durable: boolean, now: number): Promise<void>;
  unsubscribe(channel: string): Promise<void>;
  channelsForTopic(topic: string): Promise<string[]>;
  lease(request: LeaseRequest): Promise<LeasedDelivery | null>;
  ack(seq: number, token: string): Promise<boolean>;
  release(seq: number, token: string): Promise<boolean>;
  releaseOwner(owner: string): Promise<number>;
  depth(channel: string): Promise<number>;
  deadLetters(channel?: string): Promise<DeadLetter[]>;
}

export const DEAD_LETTER_MAX_DELIVERIES = 'max-deliveries-exceeded';
