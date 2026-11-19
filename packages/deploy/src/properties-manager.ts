import { readFile } from 'node:fs/promises';
import { basename, extname } from 'node:path';
import { type Logger, PropertiesRegistry, describeError, parseProperties, silentLogger } from '@integratetech/core';
import type { ScanResult, ScannedFile } from './file-scanner';

export const PROPERTIES_EXTENSION = '.properties';

export type PropertiesSnapshot = Record<string, Record<string, string>>;

export interface PropertiesChange {
  changed: string[];
  failed: string[];
}

export function propertiesName(fileName: string): string {
  const base = basename(fileName);
  return extname(base).toLowerCase() === PROPERTIES_EXTENSION ? base.slice(0, -PROPERTIES_EXTENSION.length) : base;
}

export interface PropertiesManagerOptions {
  registry?: PropertiesRegistry;
  logger?: Logger;
}

export class PropertiesManager {
  readonly registry: PropertiesRegistry;
  private readonly logger: Logger;
  private readonly readers = new Map<string, Set<string>>();
  private readonly loaded = new Set<string>();

  constructor(options: PropertiesManagerOptions = {}) {
    this.registry = options.registry ?? new PropertiesRegistry();
    this.logger = options.logger ?? silentLogger;
  }

  private async load(file: ScannedFile): Promise<boolean> {
    const name = propertiesName(file.name);
    try {
      this.registry.set(name, parseProperties(await readFile(file.path, 'utf8')));
      this.loaded.add(name);
      this.logger.info(`Loaded properties ${name} from ${file.name}`);
      return true;
    } catch (error) {
      this.logger.error(`Unable to load properties file ${file.name}: ${describeError(error)}`);
      return false;
    }
  }

  async apply(result: ScanResult): Promise<PropertiesChange> {
    const change: PropertiesChange = { changed: [], failed: [] };
    for (const file of [...result.added, ...result.modified]) {
      const name = propertiesName(file.name);
      if (await this.load(file)) change.changed.push(name);
      else change.failed.push(name);
    }
    for (const file of result.removed) {
      const name = propertiesName(file.name);
      if (!this.loaded.delete(name)) continue;
      this.registry.delete(name);
      this.logger.info(`Removed properties ${name}`);
      change.changed.push(name);
    }
    return change;
  }

  names(): string[] {
    return [...this.loaded].sort();
  }

  snapshot(): PropertiesSnapshot {
    const snapshot: PropertiesSnapshot = {};
    for (const name of this.registry.names()) snapshot[name] = { ...this.registry.get(name)! };
    return snapshot;
  }

  track(deployment: string, files: Iterable<string>): void {
    this.untrack(deployment);
    for (const file of files) {
      const readers = this.readers.get(file) ?? new Set<string>();
      readers.add(deployment);
      this.readers.set(file, readers);
    }
  }

  untrack(deployment: string): void {
    for (const [file, readers] of this.readers) {
      readers.delete(deployment);
      if (readers.size === 0) this.readers.delete(file);
    }
  }

  readersOf(file: string): string[] {
    return [...(this.readers.get(file) ?? [])].sort();
  }

  affected(files: Iterable<string>): string[] {
    const result = new Set<string>();
    for (const file of files) for (const reader of this.readers.get(file) ?? []) result.add(reader);
    return [...result].sort();
  }
}
