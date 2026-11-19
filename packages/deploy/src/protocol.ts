import { isAbsolute } from 'node:path';
import { pathToFileURL } from 'node:url';
import type { PropertiesSnapshot } from './properties-manager';
import type { RunnerOptions } from './runner';

export interface ModuleSpec {
  module: string;
  export?: string;
  options?: unknown;
}

export type LogLevel = 'debug' | 'info' | 'warn' | 'error';

export interface WorkerInit {
  name: string;
  bundle: string;
  connector: ModuleSpec;
  plugins: ModuleSpec[];
  properties: PropertiesSnapshot;
  runner: RunnerOptions;
}

export type MainToWorker = { type: 'start' } | { type: 'drain'; timeoutMs: number };

export type WorkerToMain =
  | { type: 'ready'; consumers: string[]; services: string[]; configFiles: string[] }
  | { type: 'started' }
  | { type: 'stopped'; drained: boolean }
  | { type: 'error'; phase: 'load' | 'start' | 'drain' | 'runtime'; message: string }
  | { type: 'log'; level: LogLevel; message: string };

export function moduleUrl(specifier: string): string {
  return isAbsolute(specifier) ? pathToFileURL(specifier).href : specifier;
}

export async function instantiate<T>(spec: ModuleSpec): Promise<T> {
  const loaded = (await import(moduleUrl(spec.module))) as Record<string, unknown>;
  const exportName = spec.export ?? 'default';
  const target = loaded[exportName];
  if (typeof target !== 'function') throw new Error(`Module ${spec.module} has no class export named ${exportName}`);
  return new (target as new (options?: unknown) => T)(spec.options);
}
