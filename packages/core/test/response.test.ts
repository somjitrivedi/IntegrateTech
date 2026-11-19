import { describe, expect, it } from 'vitest';
import {
  Consumer,
  Header,
  IntegrateMessage,
  Message,
  OnMessage,
  Queue,
  RESPONSE_URI,
  Response,
  ResponseTimeoutError,
  ResultResolver,
  SyncProducer,
  Topic,
  parseResponseUri,
} from '../src/index';
import { startIntegrate, waitFor } from './helpers';

class ResponseObj {
  name?: string;
  age?: number;
}

let stringResolver: ResultResolver<string>;
let mapResolver: ResultResolver<Record<string, string>>;
let objResolver: ResultResolver<ResponseObj>;

@Queue('response')
class ResponseConsumerString {
  @Message('message') m!: IntegrateMessage;
  @Message() s!: string;

  @OnMessage()
  go() {
    if (this.s === 'test') {
      stringResolver.resolve(this.m.id, this.s);
      throw new Error('This is expected');
    }
  }

  @Response()
  response() {
    return 'test';
  }
}

@Queue('response-map')
class ResponseConsumerMap {
  @Message('message') m!: IntegrateMessage;
  @Message('map') map!: Record<string, string>;

  @OnMessage()
  go() {
    if (this.map.x === 'x') {
      mapResolver.resolve(this.m.id, this.map);
      throw new Error('This is expected');
    }
  }

  @Response()
  response() {
    return { x: 'x' };
  }
}

@Queue('response-obj')
class ResponseConsumerObj {
  @Message('message') m!: IntegrateMessage;
  @Message(ResponseObj) obj!: ResponseObj;

  @OnMessage()
  go() {
    if (this.obj.name === 'Dude') {
      objResolver.resolve(this.m.id, this.obj);
      throw new Error('This is expected');
    }
  }

  @Response()
  async response() {
    await Promise.resolve();
    return { name: 'Dude', age: 22 };
  }
}

const topicReplies: string[] = [];

@Topic('replies')
class ReplyListener {
  @Message() body!: string;
  @Message('message') m!: IntegrateMessage;

  @OnMessage()
  handle() {
    topicReplies.push(`${this.m.id}:${this.body}`);
  }
}

@Queue('responder')
class TopicResponder {
  @Response()
  reply() {
    return 'pong';
  }
}

describe('request and response', () => {
  it('resolves string responses through the response uri round trip', async () => {
    const { integrate, connector } = await startIntegrate();
    stringResolver = new ResultResolver<string>();
    const producer = new SyncProducer(connector.getQueueProducer('response'), stringResolver, 'queue://response');
    const sends = await Promise.all(Array.from({ length: 20 }, () => producer.send('a')));
    await producer.close();
    const consumer = new Consumer(ResponseConsumerString, integrate, { receiveTimeoutMs: 30 });
    consumer.setOutOfMessages((idle) => {
      if (stringResolver.pendingCount() === 0) idle.shutdown();
    });
    await consumer.listen();
    const results = await Promise.all(sends.map((send) => send.result));
    expect(results).toEqual(Array(20).fill('test'));
    expect(new Set(sends.map((send) => send.correlationId)).size).toBe(20);
    await integrate.shutdown();
  });

  it('resolves key value map responses', async () => {
    const { integrate, connector } = await startIntegrate();
    mapResolver = new ResultResolver();
    const producer = new SyncProducer(connector.getQueueProducer('response-map'), mapResolver, 'queue://response-map');
    const results = await Promise.all(Array.from({ length: 5 }, () => producer.send('{}')));
    const consumer = new Consumer(ResponseConsumerMap, integrate, { receiveTimeoutMs: 30 });
    consumer.setOutOfMessages((idle) => {
      if (mapResolver.pendingCount() === 0) idle.shutdown();
    });
    await consumer.listen();
    for (const result of await Promise.all(results.map((send) => send.result))) expect(result).toEqual({ x: 'x' });
    await integrate.shutdown();
  });

  it('resolves object responses mapped into classes', async () => {
    const { integrate, connector } = await startIntegrate();
    objResolver = new ResultResolver();
    const producer = new SyncProducer(connector.getQueueProducer('response-obj'), objResolver, 'queue://response-obj');
    const pending = producer.request('{}');
    const consumer = new Consumer(ResponseConsumerObj, integrate, { receiveTimeoutMs: 30 });
    consumer.setOutOfMessages((idle) => {
      if (objResolver.pendingCount() === 0) idle.shutdown();
    });
    await consumer.listen();
    const result = await pending;
    expect(result).toBeInstanceOf(ResponseObj);
    expect(result).toMatchObject({ name: 'Dude', age: 22 });
    await integrate.shutdown();
  });

  it('publishes responses to topics with the original correlation id', async () => {
    topicReplies.length = 0;
    const { integrate, connector } = await startIntegrate();
    const [listener] = integrate.startConsumers(ReplyListener, { receiveTimeoutMs: 20 });
    await waitFor(() => connector.openConsumers() === 1);
    const id = await connector.getQueueProducer('responder').send('ping', { properties: { [RESPONSE_URI]: 'topic://replies' } });
    const responder = new Consumer(TopicResponder, integrate, { receiveTimeoutMs: 20 });
    responder.setOutOfMessages((idle) => idle.shutdown());
    await responder.listen();
    await waitFor(() => topicReplies.length === 1);
    expect(topicReplies).toEqual([`${id}:pong`]);
    await listener!.stop(1000);
    await integrate.shutdown();
  });

  it('ignores http schemes and warns about unknown schemes', async () => {
    const { integrate, connector, logger } = await startIntegrate();
    const producer = connector.getQueueProducer('responder');
    await producer.send('a', { properties: { [RESPONSE_URI]: 'https://example.test/hook' } });
    await producer.send('b', { properties: { [RESPONSE_URI]: 'ftp://files' } });
    await producer.send('c', { properties: { [RESPONSE_URI]: 'not a uri' } });
    const responder = new Consumer(TopicResponder, integrate, { receiveTimeoutMs: 20 });
    responder.setOutOfMessages((idle) => idle.shutdown());
    await responder.listen();
    expect(responder.processedCount()).toBe(3);
    expect(logger.text('warn')).toContain('Unable to find mapping for response uri scheme ftp');
    expect(logger.text('error')).toContain('Unable to determine response uri from not a uri');
    await integrate.shutdown();
  });
});

describe('ResultResolver', () => {
  it('rejects duplicates, resolves once and ignores unknown ids', async () => {
    const resolver = new ResultResolver<number>();
    const result = resolver.register('id-1');
    expect(() => resolver.register('id-1')).toThrow('Duplicate correlation id');
    expect(resolver.isPending('id-1')).toBe(true);
    expect(resolver.resolve('id-1', 7)).toBe(true);
    expect(resolver.resolve('id-1', 8)).toBe(false);
    expect(resolver.resolve(undefined, 1)).toBe(false);
    expect(await result).toBe(7);
  });

  it('times out and supports explicit rejection', async () => {
    const resolver = new ResultResolver<string>();
    const timed = resolver.register('slow', 20);
    await expect(timed).rejects.toBeInstanceOf(ResponseTimeoutError);
    expect(resolver.pendingCount()).toBe(0);
    const rejected = resolver.register('manual');
    expect(resolver.reject('manual', new Error('cancelled'))).toBe(true);
    await expect(rejected).rejects.toThrow('cancelled');
    expect(resolver.reject('manual', new Error('again'))).toBe(false);
  });
});

describe('SyncProducer', () => {
  it('stamps the response uri, applies timeouts and validates the uri', async () => {
    const { connector } = await startIntegrate();
    const inner = connector.getQueueProducer('timeouts');
    const producer = new SyncProducer<string>(inner, new ResultResolver(), 'queue://nowhere', { timeoutMs: 20 });
    expect(inner.getProperties()[RESPONSE_URI]).toBe('queue://nowhere');
    const sent = await producer.sendMap({ k: 'v' });
    await expect(sent.result).rejects.toThrow(`No response for ${sent.correlationId}`);
    const received = await (await connector.getQueue('timeouts')).receive(200);
    expect(received!.properties[RESPONSE_URI]).toBe('queue://nowhere');
    expect(received!.headers[Header.CorrelationId]).toBe(sent.correlationId);
    expect(() => new SyncProducer(inner, new ResultResolver(), 'nope')).toThrow('Invalid response uri');
    await connector.stop();
  });

  it('parses response uris', () => {
    expect(parseResponseUri('queue://a.b-c')).toEqual({ scheme: 'queue', target: 'a.b-c' });
    expect(parseResponseUri('TOPIC://Mixed_Case')).toEqual({ scheme: 'topic', target: 'Mixed_Case' });
    expect(parseResponseUri('queue:/missing')).toBeNull();
  });
});
