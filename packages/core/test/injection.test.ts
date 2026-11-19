import { describe, expect, it } from 'vitest';
import {
  BeanResolver,
  Config,
  Consumer,
  ConsumerService,
  Header,
  Headers,
  type HeaderMap,
  Inject,
  IntegrateMessage,
  Message,
  OnDeploy,
  OnMessage,
  OnUndeploy,
  Properties,
  Property,
  Queue,
} from '../src/index';
import { startIntegrate } from './helpers';

class Person {
  name?: string;
  age?: number;

  greeting() {
    return `hi ${this.name}`;
  }
}

class Mailer {
  constructor(readonly from: string) {}
}

class Clock {}

class Services {
  deployed = 0;
  undeployed = 0;
  readonly mailer = new Mailer('ops@example.test');
  readonly named = new Map<string, unknown>([['auditLog', ['entry']]]);

  @BeanResolver()
  byType(type: unknown) {
    return type === Mailer ? this.mailer : undefined;
  }

  @BeanResolver()
  byName(_type: unknown, name: string | undefined) {
    return name ? this.named.get(name) : undefined;
  }

  @OnDeploy()
  deploy() {
    this.deployed++;
  }

  @OnUndeploy()
  undeploy() {
    this.undeployed++;
  }
}

class FailingResolver {
  @BeanResolver()
  resolve(): unknown {
    throw new Error('resolver broken');
  }
}

@Queue('inject')
class FieldInjected {
  @Message() raw!: string;
  @Message('message') message!: IntegrateMessage;
  @Message('json') json!: { name: string };
  @Message(Person) person!: Person;
  @Message('map') map!: Record<string, string>;
  @Headers() headers!: HeaderMap;
  @Headers(Header.Type) type!: string;
  @Properties() props!: Record<string, string>;
  @Property() region!: string;
  @Property('trace-id') trace!: string;
  @Config('app') app!: Record<string, string>;
  @Config('app', 'timeout') timeout!: string;
  @Config() db!: Record<string, string>;
  @Config('missing') missing?: Record<string, string>;
  @Inject(Mailer) mailer!: Mailer;
  @Inject() auditLog!: string[];
  @Inject(Clock) clock?: Clock;
  untouched = 'default';

  @OnMessage()
  handle() {}
}

@Queue('inject')
class ConstructorInjected {
  constructor(
    @Message() readonly body: string,
    @Inject(Mailer) readonly mailer: Mailer,
    @Inject('auditLog') readonly audit: string[],
    readonly skipped?: unknown,
    @Property('region') readonly region?: string,
  ) {}

  @OnMessage()
  handle() {}
}

@Queue('inject')
class MapFromKeyValue {
  @Message('map') map!: Record<string, string>;
}

@Queue('inject')
class BadJson {
  @Message('json') json?: unknown;
  @Message(Person) person?: Person;
  @Message() raw!: string;
}

async function setup() {
  const context = await startIntegrate();
  context.integrate.properties.set('app', { timeout: '30', retries: '2' });
  context.integrate.properties.load('db', 'url=sqlite://local\npool:4');
  return context;
}

function message(body: string) {
  const msg = new IntegrateMessage(body);
  msg.id = 'm-1';
  msg.setHeaders({ [Header.Type]: 'person', [Header.Priority]: 5 });
  msg.setProperties({ region: 'eu', 'trace-id': 't-9' });
  return msg;
}

describe('field injection', () => {
  it('injects message, headers, properties, config and services', async () => {
    const { integrate, logger } = await setup();
    const services = new Services();
    const consumer = new Consumer(FieldInjected, integrate, { services: [services, new FailingResolver()] });
    const msg = message('{"name":"Dude","age":22}');
    const handler = (await consumer.getHandlerInstance(msg)) as FieldInjected;
    expect(handler.raw).toBe('{"name":"Dude","age":22}');
    expect(handler.message).toBe(msg);
    expect(handler.json).toEqual({ name: 'Dude', age: 22 });
    expect(handler.person).toBeInstanceOf(Person);
    expect(handler.person.greeting()).toBe('hi Dude');
    expect(handler.map).toEqual({ name: 'Dude', age: 22 });
    expect(handler.headers).toEqual({ [Header.Type]: 'person', [Header.Priority]: 5 });
    expect(handler.type).toBe('person');
    expect(handler.props).toEqual({ region: 'eu', 'trace-id': 't-9' });
    expect(handler.region).toBe('eu');
    expect(handler.trace).toBe('t-9');
    expect(handler.app).toEqual({ timeout: '30', retries: '2' });
    expect(handler.timeout).toBe('30');
    expect(handler.db).toEqual({ url: 'sqlite://local', pool: '4' });
    expect(handler.missing).toBeUndefined();
    expect(handler.mailer).toBe(services.mailer);
    expect(handler.auditLog).toEqual(['entry']);
    expect(handler.clock).toBeUndefined();
    expect(handler.untouched).toBe('default');
    expect(logger.text('warn')).toContain('resource file missing was not found');
    await integrate.shutdown();
  });

  it('parses key value bodies for map injection', async () => {
    const { integrate } = await setup();
    const handler = (await new Consumer(MapFromKeyValue, integrate).getHandlerInstance(message('a=1\nb=2'))) as MapFromKeyValue;
    expect(handler.map).toEqual({ a: '1', b: '2' });
    await integrate.shutdown();
  });

  it('warns instead of failing on bad json so the message is not redelivered forever', async () => {
    const { integrate, logger } = await setup();
    const handler = (await new Consumer(BadJson, integrate).getHandlerInstance(message('{oops'))) as BadJson;
    expect(handler.json).toBeUndefined();
    expect(handler.person).toBeUndefined();
    expect(handler.raw).toBe('{oops');
    expect(logger.text('warn')).toContain('Unable to parse json for injection');
    await integrate.shutdown();
  });
});

describe('constructor injection', () => {
  it('passes decorated parameters and leaves undecorated ones undefined', async () => {
    const { integrate } = await setup();
    const services = new Services();
    const consumer = new Consumer(ConstructorInjected, integrate, { services: [new ConsumerService(services)] });
    const handler = (await consumer.getHandlerInstance(message('hello'))) as ConstructorInjected;
    expect(handler.body).toBe('hello');
    expect(handler.mailer).toBe(services.mailer);
    expect(handler.audit).toEqual(['entry']);
    expect(handler.skipped).toBeUndefined();
    expect(handler.region).toBe('eu');
    await integrate.shutdown();
  });

  it('adds services after construction', async () => {
    const { integrate } = await setup();
    const consumer = new Consumer(ConstructorInjected, integrate);
    expect(((await consumer.getHandlerInstance(message('x'))) as ConstructorInjected).mailer).toBeUndefined();
    consumer.addServices(new Services());
    expect(((await consumer.getHandlerInstance(message('x'))) as ConstructorInjected).mailer).toBeInstanceOf(Mailer);
    consumer.setServices([]);
    expect(((await consumer.getHandlerInstance(message('x'))) as ConstructorInjected).mailer).toBeUndefined();
    await integrate.shutdown();
  });
});

describe('ConsumerService', () => {
  it('runs deploy hooks and constructs service classes', async () => {
    const service = new ConsumerService(Services);
    await service.onDeploy();
    await service.onUndeploy();
    await service.onUndeploy();
    expect((service.instance as Services).deployed).toBe(1);
    expect((service.instance as Services).undeployed).toBe(2);
    expect(await service.resolve(Mailer, undefined)).toBeInstanceOf(Mailer);
    expect(await service.resolve(Clock, 'nothing')).toBeUndefined();
  });
});
