import { describe, expect, it } from 'vitest';
import {
  GroupedTopic,
  GroupedTopicSource,
  Inject,
  MultiThreaded,
  On,
  OnMessage,
  ProcessStep,
  Queue,
  QueueSource,
  RoleInput,
  RoleInputSource,
  Topic,
  TopicSource,
  createAnnotation,
  getAnnotation,
  getSources,
  hasAnnotation,
  hasSourceAnnotation,
  matchSource,
  readMeta,
} from '../src/index';

@Queue('first')
@Topic('second')
@GroupedTopic('third', 'g')
@RoleInput()
class ManySources {
  @OnMessage()
  handle() {}
}

class Plain {}

describe('source decorators', () => {
  it('returns sources in declaration order with the context class set', () => {
    const sources = getSources(ManySources);
    expect(sources.map((source) => source.toString())).toEqual([
      'QueueSource(name=first)',
      'TopicSource(name=second)',
      'GroupedTopicSource(name=third, group=g)',
      'RoleInputSource(role=ManySources)',
    ]);
    expect(sources.every((source) => source.contextClass === ManySources)).toBe(true);
    expect(hasSourceAnnotation(ManySources)).toBe(true);
    expect(hasSourceAnnotation(Plain)).toBe(false);
    expect(getSources(Plain)).toEqual([]);
  });

  it('compares sources by value and dispatches with matchSource', () => {
    expect(new QueueSource('a').equals(new QueueSource('a'))).toBe(true);
    expect(new QueueSource('a').equals(new TopicSource('a'))).toBe(false);
    expect(new GroupedTopicSource('t', 'g').equals(new GroupedTopicSource('t', 'h'))).toBe(false);
    expect(new RoleInputSource('R').equals(new RoleInputSource('R'))).toBe(true);
    expect(new RoleInputSource().isRoleDefined()).toBe(false);
    const label = (source: Parameters<typeof matchSource>[0]) =>
      matchSource(source, { queue: () => 'q', topic: () => 't', groupedTopic: () => 'g', roleInput: () => 'r' });
    expect(getSources(ManySources).map(label)).toEqual(['q', 't', 'g', 'r']);
  });

  it('keeps metadata per class so subclasses do not inherit sources', () => {
    class Child extends ManySources {}
    expect(readMeta(Child)).toBeUndefined();
    expect(getSources(Child)).toEqual([]);
  });
});

describe('decorator validation', () => {
  it('rejects invalid MultiThreaded counts and empty On', () => {
    expect(() => MultiThreaded(0)).toThrow('positive integer');
    expect(() => MultiThreaded(1.5)).toThrow('positive integer');
    expect(() => On()).toThrow('at least one process step');
  });

  it('rejects injection on method parameters', () => {
    expect(() => {
      class Bad {
        method(@Inject() _value: unknown) {}
      }
      return Bad;
    }).toThrow('only supported on constructor parameters and fields');
  });

  it('records repeated On steps once per method', () => {
    class Hooks {
      @On(ProcessStep.Error, ProcessStep.Invalid, ProcessStep.Error)
      oops() {}
    }
    const hooks = readMeta(Hooks)!.hooks;
    expect(hooks.get(ProcessStep.Error)).toEqual(['oops']);
    expect(hooks.get(ProcessStep.Invalid)).toEqual(['oops']);
  });
});

describe('annotations', () => {
  it('stores values that plugins can read back', () => {
    const Retry = createAnnotation<{ maxRetries: number }>('test-retry');
    const Audit = createAnnotation('test-audit');
    @Retry({ maxRetries: 3 })
    class Annotated {}
    expect(hasAnnotation(Annotated, Retry)).toBe(true);
    expect(getAnnotation(Annotated, Retry)).toEqual({ maxRetries: 3 });
    expect(hasAnnotation(Annotated, Audit)).toBe(false);
    expect(Retry.annotationName).toBe('test-retry');
    expect(createAnnotation('test-retry').key).toBe(Retry.key);
  });
});
