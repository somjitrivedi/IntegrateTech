import { metaOf, readMeta } from './metadata';

export interface Annotation<T> {
  (value: T): (target: Function) => void;
  readonly key: symbol;
  readonly annotationName: string;
}

export function createAnnotation<T = true>(name: string): Annotation<T> {
  const key = Symbol.for(`integratetech.annotation.${name}`);
  const factory = ((value: T) => (target: Function) => {
    metaOf(target).annotations.set(key, value);
  }) as Annotation<T>;
  Object.defineProperty(factory, 'key', { value: key });
  Object.defineProperty(factory, 'annotationName', { value: name });
  return factory;
}

export function hasAnnotation(target: Function, annotation: Annotation<any>): boolean {
  return readMeta(target)?.annotations.has(annotation.key) ?? false;
}

export function getAnnotation<T>(target: Function, annotation: Annotation<T>): T | undefined {
  return readMeta(target)?.annotations.get(annotation.key) as T | undefined;
}
