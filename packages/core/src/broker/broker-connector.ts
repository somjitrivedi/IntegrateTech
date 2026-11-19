import { randomUUID } from 'node:crypto';
import { sleep } from '../concurrency';
import type { Connector, MessageConsumer } from '../connector';
import { ConnectorError } from '../errors';
import { Header } from '../headers';
import { type Logger, silentLogger } from '../logger';
import { IntegrateMessage } from '../message';
import { AbstractProducer, type MessageProducer, type OutboundMessage } from '../producer';
import { RoleInputConsumer } from '../role-input';
import {
  type ActionSource,
  GroupedTopicSource,
  QueueSource,
  type RoleInputSource,
  type Source,
  TopicSource,
  matchSource,
  roleQueueName,
} from '../sources';
import type { BrokerStore, DeadLetter, LeasedDelivery } from './store';

export interface BrokerConnectorOptions {
  leaseMs?: number;
  pollIntervalMs?: number;
  maxDeliveries?: number;
  defaultGroup?: string;
  clock?: () => number;
  logger?: Logger;
}

type Destination = { kind: 'queue' | 'topic'; name: string };

export function queueChannel(name: string): string {
  return `queue:${name}`;
}

export function groupChannel(topic: string, group: string): string {
  return `group:${group}:${topic}`;
}

class BrokerMessage extends IntegrateMessage {
  constructor(
    private readonly store: BrokerStore,
    private readonly delivery: LeasedDelivery,
    private readonly token: string,
  ) {
    super(delivery.message.body);
    this.id = delivery.message.id;
    this.setProperties(delivery.message.properties);
    this.setHeaders({ ...delivery.message.headers, [Header.DeliveryCount]: delivery.attempts });
  }

  get seq(): number {
    return this.delivery.seq;
  }

  get channel(): string {
    return this.delivery.channel;
  }

  override acknowledge(): Promise<boolean> {
    return this.store.ack(this.delivery.seq, this.token);
  }

  release(): Promise<boolean> {
    return this.store.release(this.delivery.seq, this.token);
  }
}

export class BrokerMessageConsumer implements MessageConsumer {
  readonly owner = randomUUID();
  private closed = false;
  private leases = 0;

  constructor(
    private readonly connector: BrokerConnector,
    readonly channel: string,
    private readonly onClose: () => Promise<void>,
  ) {}

  async receive(timeoutMs: number, signal?: AbortSignal): Promise<IntegrateMessage | null> {
    const deadline = this.connector.now() + Math.max(0, timeoutMs);
    while (!this.closed && !signal?.aborted) {
      const token = `${this.owner}:${++this.leases}`;
      const delivery = await this.connector.store.lease({
        channel: this.channel,
        owner: this.owner,
        token,
        now: this.connector.now(),
        leaseMs: this.connector.leaseMs,
        maxAttempts: this.connector.maxDeliveries,
      });
      if (delivery) return new BrokerMessage(this.connector.store, delivery, token);
      const remaining = deadline - this.connector.now();
      if (remaining <= 0) return null;
      await sleep(Math.min(this.connector.pollIntervalMs, remaining), signal);
    }
    return null;
  }

  isClosed(): boolean {
    return this.closed;
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    await this.connector.store.releaseOwner(this.owner);
    await this.onClose();
  }
}

class BrokerProducer extends AbstractProducer {
  constructor(
    private readonly connector: BrokerConnector,
    private readonly destination: Destination,
  ) {
    super();
  }

  protected publish(message: OutboundMessage): Promise<void> {
    return this.connector.publish(this.destination, message);
  }
}

export class BrokerConnector implements Connector {
  readonly leaseMs: number;
  readonly pollIntervalMs: number;
  readonly maxDeliveries: number;
  readonly defaultGroup: string;
  readonly logger: Logger;
  private readonly clock: () => number;
  private readonly consumers = new Set<BrokerMessageConsumer>();
  private started = false;

  constructor(
    readonly store: BrokerStore,
    options: BrokerConnectorOptions = {},
  ) {
    this.leaseMs = options.leaseMs ?? 30_000;
    this.pollIntervalMs = options.pollIntervalMs ?? 20;
    this.maxDeliveries = options.maxDeliveries ?? 10;
    this.defaultGroup = options.defaultGroup ?? 'integratetech';
    this.clock = options.clock ?? Date.now;
    this.logger = options.logger ?? silentLogger;
  }

  now(): number {
    return this.clock();
  }

  isStarted(): boolean {
    return this.started;
  }

  async start(): Promise<void> {
    if (this.started) return;
    try {
      await this.store.init();
    } catch (error) {
      throw new ConnectorError('Unable to start broker store', { cause: error });
    }
    this.started = true;
  }

  async stop(): Promise<void> {
    if (!this.started) return;
    for (const consumer of [...this.consumers]) await consumer.close();
    this.started = false;
    await this.store.close();
  }

  private ensureStarted(): void {
    if (!this.started) throw new ConnectorError('Connector is not started');
  }

  mapSource(source: RoleInputSource): ActionSource {
    const role = source.getRole();
    if (!role) throw new ConnectorError('Role input source has no role');
    return new QueueSource(roleQueueName(role));
  }

  async getConsumerForSource(source: Source): Promise<MessageConsumer> {
    return matchSource<Promise<MessageConsumer>>(source, {
      queue: (queue) => this.getQueue(queue.name),
      topic: (topic) => this.getTopic(topic.name),
      groupedTopic: (grouped) => this.getGroupedTopic(grouped),
      roleInput: async (role) => new RoleInputConsumer(await this.getConsumerForSource(this.mapSource(role)), this.logger),
    });
  }

  async getQueue(name: string): Promise<MessageConsumer> {
    this.ensureStarted();
    return this.track(queueChannel(name), async () => {});
  }

  async getTopic(name: string): Promise<MessageConsumer> {
    this.ensureStarted();
    const channel = `topic:${name}:${randomUUID()}`;
    await this.store.subscribe(name, channel, false, this.now());
    return this.track(channel, () => this.store.unsubscribe(channel));
  }

  async getGroupedTopic(source: GroupedTopicSource | { name: string; group?: string }): Promise<MessageConsumer> {
    this.ensureStarted();
    const group = source.group ? source.group : this.defaultGroup;
    const channel = groupChannel(source.name, group);
    await this.store.subscribe(source.name, channel, true, this.now());
    return this.track(channel, async () => {});
  }

  getQueueProducer(name: string): MessageProducer {
    return new BrokerProducer(this, { kind: 'queue', name });
  }

  getTopicProducer(name: string): MessageProducer {
    return new BrokerProducer(this, { kind: 'topic', name });
  }

  supportsOrder(): boolean {
    return true;
  }

  supportsResponse(): boolean {
    return true;
  }

  async publish(destination: Destination, message: OutboundMessage): Promise<void> {
    this.ensureStarted();
    const channels = destination.kind === 'queue' ? [queueChannel(destination.name)] : await this.store.channelsForTopic(destination.name);
    if (channels.length === 0) return;
    const now = this.now();
    await this.store.insert(
      {
        id: message.id,
        body: message.body,
        headers: message.headers,
        properties: message.properties,
        createdAt: message.createdAt,
        expiresAt: message.expiresAt,
      },
      channels,
      { availableAt: now + message.delayMs, priority: message.priority },
    );
  }

  depth(source: Source | string): Promise<number> {
    if (typeof source === 'string') return this.store.depth(queueChannel(source));
    return matchSource<Promise<number>>(source, {
      queue: (queue) => this.store.depth(queueChannel(queue.name)),
      topic: async (topic) => {
        const channels = (await this.store.channelsForTopic(topic.name)).filter((channel) => channel.startsWith('topic:'));
        let total = 0;
        for (const channel of channels) total += await this.store.depth(channel);
        return total;
      },
      groupedTopic: (grouped) => this.store.depth(groupChannel(grouped.name, grouped.group || this.defaultGroup)),
      roleInput: (role) => this.depth(this.mapSource(role)),
    });
  }

  deadLetters(source?: Source | string): Promise<DeadLetter[]> {
    if (source === undefined) return this.store.deadLetters();
    if (typeof source === 'string') return this.store.deadLetters(queueChannel(source));
    if (source instanceof QueueSource) return this.store.deadLetters(queueChannel(source.name));
    if (source instanceof GroupedTopicSource) return this.store.deadLetters(groupChannel(source.name, source.group || this.defaultGroup));
    if (source instanceof TopicSource) return this.store.deadLetters().then((all) => all.filter((d) => d.channel.startsWith(`topic:${source.name}:`)));
    return this.deadLetters(this.mapSource(source));
  }

  openConsumers(): number {
    return this.consumers.size;
  }

  private track(channel: string, cleanup: () => Promise<void>): BrokerMessageConsumer {
    const consumer: BrokerMessageConsumer = new BrokerMessageConsumer(this, channel, async () => {
      this.consumers.delete(consumer);
      await cleanup();
    });
    this.consumers.add(consumer);
    return consumer;
  }
}
