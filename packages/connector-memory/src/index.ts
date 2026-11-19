import {
  BrokerConnector,
  type BrokerConnectorOptions,
  type BrokerStore,
  DEAD_LETTER_MAX_DELIVERIES,
  type DeadLetter,
  type InsertOptions,
  type LeaseRequest,
  type LeasedDelivery,
  type StoredMessage,
} from '@integratetech/core';

interface Delivery {
  seq: number;
  channel: string;
  message: StoredMessage;
  priority: number;
  availableAt: number;
  attempts: number;
  leaseOwner: string | null;
  leaseToken: string | null;
  leaseUntil: number | null;
}

interface Subscription {
  topic: string;
  durable: boolean;
}

function copyMessage(message: StoredMessage): StoredMessage {
  return { ...message, headers: { ...message.headers }, properties: { ...message.properties } };
}

export class MemoryBrokerStore implements BrokerStore {
  private seq = 0;
  private readonly deliveries = new Map<string, Delivery[]>();
  private readonly subscriptions = new Map<string, Subscription>();
  private readonly dead: DeadLetter[] = [];

  async init(): Promise<void> {}

  async close(): Promise<void> {}

  async insert(message: StoredMessage, channels: string[], options: InsertOptions): Promise<void> {
    for (const channel of channels) {
      const list = this.deliveries.get(channel) ?? [];
      list.push({
        seq: ++this.seq,
        channel,
        message: copyMessage(message),
        priority: options.priority,
        availableAt: options.availableAt,
        attempts: 0,
        leaseOwner: null,
        leaseToken: null,
        leaseUntil: null,
      });
      this.deliveries.set(channel, list);
    }
  }

  async subscribe(topic: string, channel: string, durable: boolean): Promise<void> {
    if (!this.subscriptions.has(channel)) this.subscriptions.set(channel, { topic, durable });
  }

  async unsubscribe(channel: string): Promise<void> {
    const subscription = this.subscriptions.get(channel);
    if (!subscription || subscription.durable) return;
    this.subscriptions.delete(channel);
    this.deliveries.delete(channel);
  }

  async channelsForTopic(topic: string): Promise<string[]> {
    return [...this.subscriptions.entries()].filter(([, subscription]) => subscription.topic === topic).map(([channel]) => channel);
  }

  private remove(delivery: Delivery): void {
    const list = this.deliveries.get(delivery.channel);
    if (!list) return;
    const index = list.indexOf(delivery);
    if (index !== -1) list.splice(index, 1);
  }

  async lease(request: LeaseRequest): Promise<LeasedDelivery | null> {
    const list = this.deliveries.get(request.channel);
    if (!list) return null;
    while (true) {
      let best: Delivery | undefined;
      for (const delivery of list) {
        if (delivery.availableAt > request.now) continue;
        if (delivery.leaseToken !== null && (delivery.leaseUntil ?? 0) >= request.now) continue;
        if (!best || delivery.priority > best.priority || (delivery.priority === best.priority && delivery.seq < best.seq)) best = delivery;
      }
      if (!best) return null;
      if (best.message.expiresAt !== null && best.message.expiresAt <= request.now) {
        this.remove(best);
        continue;
      }
      if (best.attempts >= request.maxAttempts) {
        this.remove(best);
        this.dead.push({
          seq: best.seq,
          channel: best.channel,
          attempts: best.attempts,
          reason: DEAD_LETTER_MAX_DELIVERIES,
          deadAt: request.now,
          message: copyMessage(best.message),
        });
        continue;
      }
      best.attempts++;
      best.leaseOwner = request.owner;
      best.leaseToken = request.token;
      best.leaseUntil = request.now + request.leaseMs;
      return { seq: best.seq, channel: best.channel, attempts: best.attempts, message: copyMessage(best.message) };
    }
  }

  private find(seq: number, token: string): Delivery | undefined {
    for (const list of this.deliveries.values()) {
      const delivery = list.find((candidate) => candidate.seq === seq);
      if (delivery) return delivery.leaseToken === token ? delivery : undefined;
    }
    return undefined;
  }

  async ack(seq: number, token: string): Promise<boolean> {
    const delivery = this.find(seq, token);
    if (!delivery) return false;
    this.remove(delivery);
    return true;
  }

  async release(seq: number, token: string): Promise<boolean> {
    const delivery = this.find(seq, token);
    if (!delivery) return false;
    delivery.leaseOwner = null;
    delivery.leaseToken = null;
    delivery.leaseUntil = null;
    return true;
  }

  async releaseOwner(owner: string): Promise<number> {
    let released = 0;
    for (const list of this.deliveries.values()) {
      for (const delivery of list) {
        if (delivery.leaseOwner !== owner) continue;
        delivery.leaseOwner = null;
        delivery.leaseToken = null;
        delivery.leaseUntil = null;
        released++;
      }
    }
    return released;
  }

  async depth(channel: string): Promise<number> {
    return this.deliveries.get(channel)?.length ?? 0;
  }

  async deadLetters(channel?: string): Promise<DeadLetter[]> {
    return this.dead.filter((letter) => channel === undefined || letter.channel === channel).map((letter) => ({ ...letter, message: copyMessage(letter.message) }));
  }
}

export class MemoryConnector extends BrokerConnector {
  declare readonly store: MemoryBrokerStore;

  constructor(options: BrokerConnectorOptions = {}) {
    super(new MemoryBrokerStore(), options);
  }
}
