import { describe, expect, it } from 'vitest';
import {
  type Connector,
  Consumer,
  ConsumerThread,
  GroupedTopic,
  IntegrateTech,
  type MessageConsumer,
  MultiThreaded,
  OnMessage,
  Order,
  Property,
  Queue,
  Response,
  RoleInput,
  Semaphore,
  Topic,
  sleep,
} from '../src/index';
import { deferred, recordingLogger, runUntilIdle, startIntegrate, waitFor } from './helpers';

let active = 0;
let peak = 0;
const order: string[] = [];

@Queue('parallel')
@MultiThreaded(3)
class Parallel {
  @OnMessage()
  async handle() {
    active++;
    peak = Math.max(peak, active);
    await sleep(25);
    active--;
  }
}

@Queue('serial')
class Serial {
  @OnMessage()
  async handle() {
    active++;
    peak = Math.max(peak, active);
    await sleep(5);
    active--;
  }
}

@Queue('ordered')
@MultiThreaded(4)
class Ordered {
  @Property('account') account!: string;
  @Property('step') step!: string;

  @Order()
  orderKey() {
    return this.account;
  }

  @OnMessage()
  async handle() {
    await sleep(this.step === '1' ? 30 : 2);
    order.push(`${this.account}-${this.step}`);
  }
}

@Queue('a')
@Topic('b')
class TwoSources {
  @OnMessage()
  handle() {}
}

class NoDecorators {}

@Queue('needs-response')
class NeedsResponse {
  @Response()
  reply() {
    return 'x';
  }
}

@Queue('needs-order')
class NeedsOrder {
  @Order()
  key() {
    return 'k';
  }
}

describe('Consumer construction', () => {
  it('requires exactly one source unless one is given', async () => {
    const { integrate } = await startIntegrate();
    expect(() => new Consumer(TwoSources, integrate)).toThrow('One consumer instance cannot consume more than one source');
    expect(() => new Consumer(NoDecorators as never, integrate)).toThrow('no decorators found');
    const consumers = integrate.createConsumers(TwoSources);
    expect(consumers.map((consumer) => consumer.getSource().toString())).toEqual(['QueueSource(name=a)', 'TopicSource(name=b)']);
    expect(consumers[0]!.getRoleSources()).toHaveLength(2);
    expect(consumers[1]!.getRoleSources('topic')).toHaveLength(1);
    expect(consumers[0]!.name).toMatch(/^a:\d+$/);
    expect(() => integrate.createConsumers(NoDecorators as never)).toThrow('at least one source');
    await integrate.shutdown();
  });

  it('rejects @Response and @Order on connectors that do not support them', async () => {
    const integrate = new IntegrateTech({ logger: recordingLogger() });
    const limited = {
      start: async () => {},
      stop: async () => {},
      supportsOrder: () => false,
      supportsResponse: () => false,
    } as unknown as Connector;
    await integrate.start(limited);
    expect(() => new Consumer(NeedsResponse, integrate)).toThrow('@Response is not supported by the current connector');
    expect(() => new Consumer(NeedsOrder, integrate)).toThrow('@Order is not supported by the current connector');
  });
});

describe('Consumer concurrency', () => {
  it('runs up to the MultiThreaded count at once', async () => {
    active = 0;
    peak = 0;
    const { integrate, connector } = await startIntegrate();
    const producer = connector.getQueueProducer('parallel');
    for (let i = 0; i < 9; i++) await producer.send(`p-${i}`);
    const consumer = new Consumer(Parallel, integrate, { receiveTimeoutMs: 40 });
    await runUntilIdle(consumer);
    expect(consumer.processedCount()).toBe(9);
    expect(peak).toBe(3);
    expect(await connector.depth('parallel')).toBe(0);
    await integrate.shutdown();
  });

  it('processes one message at a time without MultiThreaded', async () => {
    active = 0;
    peak = 0;
    const { integrate, connector } = await startIntegrate();
    for (let i = 0; i < 5; i++) await connector.getQueueProducer('serial').send(`s-${i}`);
    await runUntilIdle(new Consumer(Serial, integrate, { receiveTimeoutMs: 30 }));
    expect(peak).toBe(1);
    await integrate.shutdown();
  });

  it('keeps messages with the same order key in arrival order while others run in parallel', async () => {
    order.length = 0;
    const { integrate, connector } = await startIntegrate();
    const producer = connector.getQueueProducer('ordered');
    for (const [account, step] of [
      ['A', '1'],
      ['B', '1'],
      ['A', '2'],
      ['A', '3'],
      ['B', '2'],
    ] as const)
      await producer.send('x', { properties: { account, step } });
    await runUntilIdle(new Consumer(Ordered, integrate, { receiveTimeoutMs: 60 }));
    const forAccount = (account: string) => order.filter((entry) => entry.startsWith(account));
    expect(forAccount('A')).toEqual(['A-1', 'A-2', 'A-3']);
    expect(forAccount('B')).toEqual(['B-1', 'B-2']);
    expect(order).toHaveLength(5);
    await integrate.shutdown();
  });

  it('drains in-flight work on shutdown and leaves unreceived messages queued', async () => {
    const gate = deferred();
    const started: string[] = [];
    @Queue('drain')
    @MultiThreaded(2)
    class Slow {
      @Property('n') n!: string;

      @OnMessage()
      async handle() {
        started.push(this.n);
        await gate.promise;
      }
    }
    const { integrate, connector } = await startIntegrate();
    for (let i = 0; i < 3; i++) await connector.getQueueProducer('drain').send('x', { properties: { n: String(i) } });
    const consumer = new Consumer(Slow, integrate, { receiveTimeoutMs: 20 });
    const listening = consumer.listen();
    await waitFor(() => started.length >= 2);
    consumer.shutdown();
    let finished = false;
    void listening.then(() => {
      finished = true;
    });
    await sleep(20);
    expect(finished).toBe(false);
    gate.resolve();
    await listening;
    expect(consumer.isRunning()).toBe(false);
    expect(await connector.depth('drain')).toBe(3 - consumer.processedCount());
    await integrate.shutdown();
  });

  it('stops listening when a handler cannot be prepared', async () => {
    @Queue('broken-construct')
    class Unbuildable {
      constructor() {
        throw new Error('ctor failed');
      }
    }
    const { integrate, connector, logger } = await startIntegrate();
    await connector.getQueueProducer('broken-construct').send('x');
    const consumer = new Consumer(Unbuildable, integrate, { receiveTimeoutMs: 20 });
    await consumer.listen();
    expect(logger.text('error')).toContain("couldn't be used: ctor failed");
    expect(await connector.depth('broken-construct')).toBe(1);
    await integrate.shutdown();
  });
});

describe('ConsumerThread', () => {
  it('reconnects with backoff after listen fails and stops on shutdown', async () => {
    const { integrate, connector, logger } = await startIntegrate();
    let failures = 2;
    const original = connector.getConsumerForSource.bind(connector);
    connector.getConsumerForSource = async (source): Promise<MessageConsumer> => {
      if (failures-- > 0) throw new Error('broker unavailable');
      return original(source);
    };
    const handled: string[] = [];
    @Queue('thread')
    class Threaded {
      @Property('n') n!: string;

      @OnMessage()
      handle() {
        handled.push(this.n);
      }
    }
    await connector.getQueueProducer('thread').send('x', { properties: { n: '1' } });
    const thread = new ConsumerThread(new Consumer(Threaded, integrate, { receiveTimeoutMs: 20 }), { backoffMs: [5, 10] }).start();
    await waitFor(() => handled.length === 1);
    expect(thread.connectAttempts()).toBe(3);
    expect(logger.text('error')).toContain("couldn't get connection: broker unavailable");
    expect(logger.text('warn')).toContain('attempt 2');
    expect(await thread.stop(1000)).toBe(true);
    expect(thread.isRunning()).toBe(false);
    await integrate.shutdown();
  });

  it('starts one thread per source and consumes role input alongside the queue', async () => {
    const seen: string[] = [];
    @Queue('multi-source')
    @GroupedTopic('multi-topic', 'grp')
    @RoleInput('Multi')
    class MultiSource {
      @Property('from') from!: string;

      @OnMessage()
      handle() {
        seen.push(this.from);
      }
    }
    const { integrate, connector } = await startIntegrate();
    const threads = integrate.startConsumers(MultiSource, { receiveTimeoutMs: 20, backoffMs: [5] });
    expect(threads).toHaveLength(3);
    await waitFor(() => connector.openConsumers() === 3);
    await connector.getQueueProducer('multi-source').send('x', { properties: { from: 'queue' } });
    await connector.getTopicProducer('multi-topic').send('x', { properties: { from: 'topic' } });
    await connector.getQueueProducer('integrate-role-Multi').send(JSON.stringify({ role: 'Multi', id: 'r', msg: 'x', properties: { from: 'role' } }));
    await waitFor(() => seen.length === 3);
    expect(seen.sort()).toEqual(['queue', 'role', 'topic']);
    await Promise.all(threads.map((thread) => thread.stop(1000)));
    await integrate.shutdown();
  });
});

describe('Semaphore', () => {
  it('hands permits to waiters in order', async () => {
    const semaphore = new Semaphore(1);
    const log: number[] = [];
    await semaphore.acquire();
    const second = semaphore.acquire().then(() => log.push(2));
    const third = semaphore.acquire().then(() => log.push(3));
    expect(semaphore.waiting).toBe(2);
    semaphore.release();
    await second;
    semaphore.release();
    await third;
    semaphore.release();
    expect(log).toEqual([2, 3]);
    expect(semaphore.active).toBe(0);
    expect(() => new Semaphore(0)).toThrow();
  });
});
