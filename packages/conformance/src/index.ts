import {
  type BrokerConnector,
  type BrokerConnectorOptions,
  DEAD_LETTER_MAX_DELIVERIES,
  GroupedTopicSource,
  Header,
  IntegrateMessage,
  type MessageConsumer,
  QueueSource,
  RoleInputMessage,
  RoleInputSource,
  TopicSource,
  roleQueueName,
} from '@integratetech/core';
import { afterEach, describe, expect, it } from 'vitest';

export type ConnectorFactory = (options: BrokerConnectorOptions) => BrokerConnector | Promise<BrokerConnector>;

const BASE: BrokerConnectorOptions = { leaseMs: 200, pollIntervalMs: 5, maxDeliveries: 3 };

async function drain(consumer: MessageConsumer, timeoutMs = 50): Promise<IntegrateMessage[]> {
  const received: IntegrateMessage[] = [];
  while (true) {
    const message = await consumer.receive(timeoutMs);
    if (!message) return received;
    received.push(message);
    await message.acknowledge();
  }
}

export function defineConnectorConformance(name: string, factory: ConnectorFactory): void {
  describe(`${name} connector conformance`, () => {
    const open: BrokerConnector[] = [];

    async function connect(options: BrokerConnectorOptions = {}): Promise<BrokerConnector> {
      const connector = await factory({ ...BASE, ...options });
      await connector.start();
      open.push(connector);
      return connector;
    }

    afterEach(async () => {
      while (open.length > 0) await open.pop()!.stop();
    });

    it('delivers a queued string with id, headers and properties', async () => {
      const connector = await connect();
      const producer = connector.getQueueProducer('orders');
      const id = await producer.send('hello', { properties: { region: 'eu' }, headers: { [Header.Type]: 'greeting' } });
      const consumer = await connector.getQueue('orders');
      const message = await consumer.receive(500);
      expect(message).not.toBeNull();
      expect(message!.id).toBe(id);
      expect(message!.body).toBe('hello');
      expect(message!.properties).toEqual({ region: 'eu' });
      expect(message!.headers[Header.Type]).toBe('greeting');
      expect(message!.headers[Header.CorrelationId]).toBe(id);
      expect(message!.headers[Header.DeliveryCount]).toBe(1);
      expect(typeof message!.headers[Header.Timestamp]).toBe('number');
      expect(await message!.acknowledge()).toBe(true);
      expect(await connector.depth('orders')).toBe(0);
    });

    it('serializes objects as json and maps as key value lines', async () => {
      const connector = await connect();
      const producer = connector.getQueueProducer('bodies');
      await producer.send({ name: 'Dude', age: 22 });
      await producer.sendMap({ a: '1', b: 'two' });
      await producer.sendMap(new Map([['c', '3']]));
      const bodies = (await drain(await connector.getQueue('bodies'))).map((message) => message.body);
      expect(bodies).toEqual(['{"name":"Dude","age":22}', 'a=1\nb=two\n', 'c=3\n']);
    });

    it('keeps an explicit message id and applies producer level headers and properties', async () => {
      const connector = await connect();
      const producer = connector.getQueueProducer('defaults');
      producer.setProperties({ source: 'producer', shared: 'producer' });
      producer.setHeaders({ [Header.Producer]: 'billing' });
      const message = new IntegrateMessage('payload');
      message.id = 'fixed-id';
      message.setProperties({ shared: 'message' });
      const id = await producer.send(message, { properties: { extra: 'call' } });
      expect(id).toBe('fixed-id');
      const received = await (await connector.getQueue('defaults')).receive(500);
      expect(received!.id).toBe('fixed-id');
      expect(received!.properties).toEqual({ source: 'producer', shared: 'message', extra: 'call' });
      expect(received!.headers[Header.Producer]).toBe('billing');
    });

    it('rejects sends on a closed producer', async () => {
      const connector = await connect();
      const producer = connector.getQueueProducer('closed');
      await producer.close();
      await expect(producer.send('x')).rejects.toThrow('Producer is closed');
    });

    it('delivers queue messages in fifo order within a priority and higher priority first', async () => {
      const connector = await connect();
      const producer = connector.getQueueProducer('priority');
      await producer.send('low-1', { headers: { [Header.Priority]: 1 } });
      await producer.send('normal-1');
      await producer.send('normal-2');
      await producer.send('high-1', { headers: { [Header.Priority]: 9 } });
      const bodies = (await drain(await connector.getQueue('priority'))).map((message) => message.body);
      expect(bodies).toEqual(['high-1', 'normal-1', 'normal-2', 'low-1']);
    });

    it('gives each queued message to exactly one competing consumer', async () => {
      const connector = await connect();
      const producer = connector.getQueueProducer('work');
      const sent = new Set<string>();
      for (let i = 0; i < 40; i++) sent.add(await producer.send(`job-${i}`));
      const first = await connector.getQueue('work');
      const second = await connector.getQueue('work');
      const seen: string[] = [];
      const take = async (consumer: MessageConsumer) => {
        while (true) {
          const message = await consumer.receive(40);
          if (!message) return;
          seen.push(message.id!);
          await message.acknowledge();
        }
      };
      await Promise.all([take(first), take(second)]);
      expect(seen).toHaveLength(40);
      expect(new Set(seen)).toEqual(sent);
    });

    it('fans topic messages out to every subscriber that exists at publish time', async () => {
      const connector = await connect();
      const producer = connector.getTopicProducer('prices');
      await producer.send('before-subscribers');
      const a = await connector.getTopic('prices');
      const b = await connector.getTopic('prices');
      await producer.send('tick');
      expect((await drain(a)).map((message) => message.body)).toEqual(['tick']);
      expect((await drain(b)).map((message) => message.body)).toEqual(['tick']);
      await a.close();
      await producer.send('after-close');
      expect((await drain(b)).map((message) => message.body)).toEqual(['after-close']);
      expect(await connector.depth(new TopicSource('prices'))).toBe(0);
    });

    it('delivers grouped topic messages once per group and shares them inside a group', async () => {
      const connector = await connect();
      const billingA = await connector.getConsumerForSource(new GroupedTopicSource('users', 'billing'));
      const billingB = await connector.getConsumerForSource(new GroupedTopicSource('users', 'billing'));
      const audit = await connector.getConsumerForSource(new GroupedTopicSource('users', 'audit'));
      const producer = connector.getTopicProducer('users');
      for (let i = 0; i < 10; i++) await producer.send(`user-${i}`);
      const [fromA, fromB] = await Promise.all([drain(billingA), drain(billingB)]);
      const billing = [...fromA, ...fromB].map((message) => message.body).sort();
      expect(billing).toHaveLength(10);
      expect(new Set(billing).size).toBe(10);
      expect((await drain(audit)).map((message) => message.body)).toHaveLength(10);
    });

    it('keeps grouped topic messages for a group while no consumer is connected', async () => {
      const connector = await connect();
      const first = await connector.getConsumerForSource(new GroupedTopicSource('events', 'durable'));
      await first.close();
      await connector.getTopicProducer('events').send('while-away');
      const again = await connector.getConsumerForSource(new GroupedTopicSource('events', 'durable'));
      expect((await drain(again)).map((message) => message.body)).toEqual(['while-away']);
    });

    it('uses the default group when a grouped topic has no group', async () => {
      const connector = await connect({ defaultGroup: 'shared-default' });
      const unnamed = await connector.getConsumerForSource(new GroupedTopicSource('alerts'));
      const named = await connector.getConsumerForSource(new GroupedTopicSource('alerts', 'shared-default'));
      await connector.getTopicProducer('alerts').send('one');
      const total = (await drain(unnamed)).length + (await drain(named)).length;
      expect(total).toBe(1);
    });

    it('redelivers an unacknowledged message after its lease expires', async () => {
      const connector = await connect({ leaseMs: 60 });
      await connector.getQueueProducer('lease').send('retry-me');
      const consumer = await connector.getQueue('lease');
      const first = await consumer.receive(500);
      expect(first!.headers[Header.DeliveryCount]).toBe(1);
      expect(await consumer.receive(20)).toBeNull();
      const second = await consumer.receive(1000);
      expect(second!.id).toBe(first!.id);
      expect(second!.headers[Header.DeliveryCount]).toBe(2);
      expect(await first!.acknowledge()).toBe(false);
      expect(await second!.acknowledge()).toBe(true);
      expect(await second!.acknowledge()).toBe(false);
    });

    it('releases leases when a consumer closes so another consumer gets the message', async () => {
      const connector = await connect({ leaseMs: 60_000 });
      await connector.getQueueProducer('handoff').send('in-flight');
      const first = await connector.getQueue('handoff');
      const leased = await first.receive(500);
      expect(leased).not.toBeNull();
      await first.close();
      const second = await connector.getQueue('handoff');
      const again = await second.receive(500);
      expect(again!.body).toBe('in-flight');
      expect(again!.headers[Header.DeliveryCount]).toBe(2);
    });

    it('moves a message to the dead letter list after max deliveries', async () => {
      const connector = await connect({ leaseMs: 10, maxDeliveries: 2 });
      await connector.getQueueProducer('poison').send('bad');
      const consumer = await connector.getQueue('poison');
      expect(await consumer.receive(500)).not.toBeNull();
      expect(await consumer.receive(500)).not.toBeNull();
      expect(await consumer.receive(100)).toBeNull();
      const dead = await connector.deadLetters('poison');
      expect(dead).toHaveLength(1);
      expect(dead[0]!.message.body).toBe('bad');
      expect(dead[0]!.attempts).toBe(2);
      expect(dead[0]!.reason).toBe(DEAD_LETTER_MAX_DELIVERIES);
      expect(await connector.depth('poison')).toBe(0);
      expect(await connector.deadLetters(new QueueSource('other'))).toHaveLength(0);
    });

    it('holds delayed messages until they are due', async () => {
      const connector = await connect();
      await connector.getQueueProducer('delayed').send('later', { delayMs: 150 });
      const consumer = await connector.getQueue('delayed');
      expect(await consumer.receive(30)).toBeNull();
      const message = await consumer.receive(1000);
      expect(message!.body).toBe('later');
    });

    it('drops messages whose expiration has passed', async () => {
      const connector = await connect();
      const producer = connector.getQueueProducer('ttl');
      await producer.send('stale', { headers: { [Header.Expiration]: Date.now() - 1 } });
      await producer.send('fresh', { headers: { [Header.Expiration]: Date.now() + 60_000 } });
      const bodies = (await drain(await connector.getQueue('ttl'))).map((message) => message.body);
      expect(bodies).toEqual(['fresh']);
    });

    it('returns null on timeout and stops waiting when aborted', async () => {
      const connector = await connect();
      const consumer = await connector.getQueue('empty');
      const started = Date.now();
      expect(await consumer.receive(40)).toBeNull();
      expect(Date.now() - started).toBeGreaterThanOrEqual(30);
      const controller = new AbortController();
      setTimeout(() => controller.abort(), 20);
      const abortedAt = Date.now();
      expect(await consumer.receive(5000, controller.signal)).toBeNull();
      expect(Date.now() - abortedAt).toBeLessThan(1000);
    });

    it('maps role input sources to the role queue and unwraps the envelope', async () => {
      const connector = await connect();
      const source = new RoleInputSource('Billing');
      expect(connector.mapSource(source)).toEqual(new QueueSource(roleQueueName('Billing')));
      const original = new IntegrateMessage('{"amount":5}');
      original.id = 'original-id';
      original.setProperties({ attempt: '3' });
      original.setHeaders({ [Header.Type]: 'invoice' });
      await connector.getQueueProducer(roleQueueName('Billing')).send(RoleInputMessage.fromMessage('Billing', original).toString());
      const consumer = await connector.getConsumerForSource(source);
      const message = await consumer.receive(500);
      expect(message!.id).toBe('original-id');
      expect(message!.body).toBe('{"amount":5}');
      expect(message!.properties).toEqual({ attempt: '3' });
      expect(message!.headers[Header.Type]).toBe('invoice');
      expect(await message!.acknowledge()).toBe(true);
      expect(await connector.depth(source)).toBe(0);
    });

    it('drops malformed role input envelopes', async () => {
      const connector = await connect();
      const producer = connector.getQueueProducer(roleQueueName('Strict'));
      await producer.send('not json');
      await producer.send(new RoleInputMessage('Strict', 'ok-id', 'fine').toString());
      const consumer = await connector.getConsumerForSource(new RoleInputSource('Strict'));
      const message = await consumer.receive(500);
      expect(message!.id).toBe('ok-id');
      await message!.acknowledge();
      expect(await connector.depth(roleQueueName('Strict'))).toBe(0);
    });

    it('refuses to work before start and closes consumers on stop', async () => {
      const connector = await factory(BASE);
      await expect(connector.getQueue('x')).rejects.toThrow('Connector is not started');
      await connector.start();
      await connector.getQueue('x');
      await connector.getTopic('y');
      expect(connector.openConsumers()).toBe(2);
      await connector.stop();
      expect(connector.openConsumers()).toBe(0);
      expect(connector.supportsOrder()).toBe(true);
      expect(connector.supportsResponse()).toBe(true);
    });
  });
}
