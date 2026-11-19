import type { Annotation } from './annotations';
import type { Header } from './headers';
import { type InjectionSpec, type MemberKey, type MessageAs, type SourceSpec, metaOf, pushUnique } from './metadata';
import type { ProcessStep } from './process-step';

type ClassDecorator = (target: Function) => void;
type MethodDecorator = (target: object, key: MemberKey, descriptor?: PropertyDescriptor) => void;
type InjectionDecorator = (target: object, key: MemberKey | undefined, index?: number) => void;

function ownerOf(target: object): Function {
  return typeof target === 'function' ? target : (target as { constructor: Function }).constructor;
}

function addSource(spec: SourceSpec): ClassDecorator {
  return (target) => {
    metaOf(target).sources.unshift(spec);
  };
}

export function Queue(name: string): ClassDecorator {
  return addSource({ kind: 'queue', name });
}

export function Topic(name: string): ClassDecorator {
  return addSource({ kind: 'topic', name });
}

export function GroupedTopic(name: string, group = ''): ClassDecorator {
  return addSource({ kind: 'groupedTopic', name, group });
}

export function RoleInput(role = ''): ClassDecorator {
  return addSource({ kind: 'roleInput', role });
}

export function MultiThreaded(threads: number): ClassDecorator {
  if (!Number.isInteger(threads) || threads < 1) throw new Error('MultiThreaded requires a positive integer');
  return (target) => {
    metaOf(target).multiThreaded = threads;
  };
}

export function Service(): ClassDecorator {
  return (target) => {
    metaOf(target).service = true;
  };
}

export function CoreService(): ClassDecorator {
  return (target) => {
    metaOf(target).coreService = true;
  };
}

function methodList(select: (meta: ReturnType<typeof metaOf>) => MemberKey[]): () => MethodDecorator {
  return () => (target, key) => {
    pushUnique(select(metaOf(ownerOf(target))), key);
  };
}

export const OnMessage = methodList((meta) => meta.onMessage);
export const OnValidate = methodList((meta) => meta.onValidate);
export const Response = methodList((meta) => meta.onResponse);
export const BeanResolver = methodList((meta) => meta.beanResolvers);
export const OnDeploy = methodList((meta) => meta.onDeploy);
export const OnUndeploy = methodList((meta) => meta.onUndeploy);

export function Order(): MethodDecorator {
  return (target, key) => {
    metaOf(ownerOf(target)).order = key;
  };
}

export function On(...steps: ProcessStep[]): MethodDecorator {
  if (steps.length === 0) throw new Error('On requires at least one process step');
  return (target, key) => {
    const hooks = metaOf(ownerOf(target)).hooks;
    for (const step of new Set(steps)) {
      const list = hooks.get(step) ?? [];
      pushUnique(list, key);
      hooks.set(step, list);
    }
  };
}

export function LifeCycle(step: ProcessStep, annotation?: Annotation<any>): MethodDecorator {
  return (target, key) => {
    metaOf(ownerOf(target)).lifecycle.push({
      step,
      method: key,
      isStatic: typeof target === 'function',
      ...(annotation ? { annotation: { key: annotation.key, annotationName: annotation.annotationName } } : {}),
    });
  };
}

function injection(build: (fieldName: string | undefined) => InjectionSpec): InjectionDecorator {
  return (target, key, index) => {
    if (typeof index === 'number') {
      if (key !== undefined) throw new Error('Injection decorators are only supported on constructor parameters and fields');
      metaOf(target as Function).params.set(index, build(undefined));
      return;
    }
    if (key === undefined) throw new Error('Injection decorators require a field or constructor parameter');
    metaOf(ownerOf(target)).fields.push({ key, spec: build(typeof key === 'string' ? key : undefined) });
  };
}

export function Message(as: MessageAs = 'string'): InjectionDecorator {
  return injection(() => ({ kind: 'message', as }));
}

export function Headers(header?: Header): InjectionDecorator {
  return injection(() => (header ? { kind: 'headers', header } : { kind: 'headers' }));
}

export function Properties(): InjectionDecorator {
  return injection(() => ({ kind: 'properties' }));
}

export function Property(name?: string): InjectionDecorator {
  return injection((fieldName) => {
    const resolved = name ?? fieldName;
    if (!resolved) throw new Error('Property requires a name when used on a constructor parameter');
    return { kind: 'properties', name: resolved };
  });
}

export function Config(file?: string, field?: string): InjectionDecorator {
  return injection((fieldName) => {
    const resolvedFile = file ?? fieldName;
    if (!resolvedFile) throw new Error('Config requires a file name when used on a constructor parameter');
    return field ? { kind: 'config', file: resolvedFile, field } : { kind: 'config', file: resolvedFile };
  });
}

export function Producer(target: { queue: string } | { topic: string }): InjectionDecorator {
  return injection(() => ('queue' in target ? { kind: 'producer', queue: target.queue } : { kind: 'producer', topic: target.topic }));
}

export function Inject(type?: unknown, name?: string): InjectionDecorator {
  return injection((fieldName) => {
    if (typeof type === 'string') return { kind: 'inject', name: type };
    const resolvedName = name ?? fieldName;
    return { kind: 'inject', ...(type !== undefined ? { type } : {}), ...(resolvedName ? { name: resolvedName } : {}) };
  });
}
