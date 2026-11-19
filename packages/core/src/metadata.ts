import type { Header } from './headers';
import type { ProcessStep } from './process-step';
import { GroupedTopicSource, QueueSource, RoleInputSource, type Source, TopicSource } from './sources';

export type MemberKey = string | symbol;

export type MessageAs = 'string' | 'json' | 'map' | 'message' | (new () => object);

export type InjectionSpec =
  | { kind: 'message'; as: MessageAs }
  | { kind: 'headers'; header?: Header }
  | { kind: 'properties'; name?: string }
  | { kind: 'config'; file?: string; field?: string }
  | { kind: 'producer'; queue?: string; topic?: string }
  | { kind: 'inject'; type?: unknown; name?: string };

export type SourceSpec =
  | { kind: 'queue'; name: string }
  | { kind: 'topic'; name: string }
  | { kind: 'groupedTopic'; name: string; group: string }
  | { kind: 'roleInput'; role: string };

export interface LifeCycleBinding {
  step: ProcessStep;
  method: MemberKey;
  isStatic: boolean;
  annotation?: { key: symbol; annotationName: string };
}

export interface ClassMeta {
  sources: SourceSpec[];
  onMessage: MemberKey[];
  onValidate: MemberKey[];
  onResponse: MemberKey[];
  order?: MemberKey;
  hooks: Map<ProcessStep, MemberKey[]>;
  fields: { key: MemberKey; spec: InjectionSpec }[];
  params: Map<number, InjectionSpec>;
  multiThreaded?: number;
  annotations: Map<symbol, unknown>;
  lifecycle: LifeCycleBinding[];
  beanResolvers: MemberKey[];
  onDeploy: MemberKey[];
  onUndeploy: MemberKey[];
  service: boolean;
  coreService: boolean;
}

const META = Symbol.for('integratetech.meta');

function emptyMeta(): ClassMeta {
  return {
    sources: [],
    onMessage: [],
    onValidate: [],
    onResponse: [],
    hooks: new Map(),
    fields: [],
    params: new Map(),
    annotations: new Map(),
    lifecycle: [],
    beanResolvers: [],
    onDeploy: [],
    onUndeploy: [],
    service: false,
    coreService: false,
  };
}

export function readMeta(target: Function): ClassMeta | undefined {
  return Object.prototype.hasOwnProperty.call(target, META) ? ((target as any)[META] as ClassMeta) : undefined;
}

export function metaOf(target: Function): ClassMeta {
  const existing = readMeta(target);
  if (existing) return existing;
  const meta = emptyMeta();
  Object.defineProperty(target, META, { value: meta, enumerable: false });
  return meta;
}

export function pushUnique<T>(list: T[], value: T): void {
  if (!list.includes(value)) list.push(value);
}

export function sourceFromSpec(spec: SourceSpec): Source {
  switch (spec.kind) {
    case 'queue':
      return new QueueSource(spec.name);
    case 'topic':
      return new TopicSource(spec.name);
    case 'groupedTopic':
      return new GroupedTopicSource(spec.name, spec.group);
    case 'roleInput':
      return new RoleInputSource(spec.role);
  }
}

export function getSources(target: Function): Source[] {
  const meta = readMeta(target);
  if (!meta) return [];
  return meta.sources.map((spec) => sourceFromSpec(spec).setContextClass(target));
}

export function hasSourceAnnotation(target: Function): boolean {
  return (readMeta(target)?.sources.length ?? 0) > 0;
}

export function getConfigFiles(target: Function): string[] {
  const meta = readMeta(target);
  if (!meta) return [];
  const files = new Set<string>();
  for (const spec of [...meta.fields.map((field) => field.spec), ...meta.params.values()]) {
    if (spec.kind === 'config' && spec.file) files.add(spec.file);
  }
  return [...files];
}
