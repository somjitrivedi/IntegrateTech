import { rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { Config, OnMessage, PropertiesRegistry, Queue, Service, getConfigFiles } from '@integratetech/core';
import { afterEach, describe, expect, it } from 'vitest';
import { FileScanner, PropertiesManager, configFilesOf, extensionFilter, inspectClasses, propertiesName } from '../src/index';
import { recordingLogger, workDir } from './helpers';

let cleanup: (() => void) | undefined;

afterEach(() => {
  cleanup?.();
  cleanup = undefined;
});

@Queue('a')
class ReadsApp {
  @Config('app') app!: Record<string, string>;
  @Config('db', 'url') url!: string;

  @OnMessage()
  handle() {}
}

@Service()
class ReadsShared {
  constructor(@Config('shared') readonly shared: Record<string, string>) {}
}

describe('PropertiesManager', () => {
  it('names properties files without their extension', () => {
    expect(propertiesName('app.properties')).toBe('app');
    expect(propertiesName('/x/y/App.PROPERTIES')).toBe('App');
    expect(propertiesName('other.txt')).toBe('other.txt');
  });

  it('loads, reloads and removes files in the registry', async () => {
    const dir = workDir('props');
    cleanup = dir.cleanup;
    const registry = new PropertiesRegistry();
    const manager = new PropertiesManager({ registry, logger: recordingLogger() });
    const scanner = new FileScanner(dir.propertiesDir, { filter: extensionFilter('.properties') });
    const file = join(dir.propertiesDir, 'app.properties');
    writeFileSync(file, 'greeting = hello\nmulti = a\\\n  b\n');
    await scanner.scan();
    expect(await manager.apply(await scanner.scan())).toEqual({ changed: ['app'], failed: [] });
    expect(registry.get('app')).toEqual({ greeting: 'hello', multi: 'ab' });
    expect(manager.names()).toEqual(['app']);

    writeFileSync(file, 'greeting=bonjour\nextra=1\n');
    await scanner.scan();
    expect((await manager.apply(await scanner.scan())).changed).toEqual(['app']);
    expect(manager.snapshot()).toEqual({ app: { greeting: 'bonjour', extra: '1' } });

    rmSync(file);
    expect((await manager.apply(await scanner.scan())).changed).toEqual(['app']);
    expect(registry.get('app')).toBeUndefined();
    expect(manager.snapshot()).toEqual({});
  });

  it('reports files it could not read and leaves other names alone', async () => {
    const logger = recordingLogger();
    const registry = new PropertiesRegistry();
    registry.set('manual', { a: '1' });
    const manager = new PropertiesManager({ registry, logger });
    const change = await manager.apply({ dir: 'x', added: [{ name: 'gone.properties', path: '/does/not/exist/gone.properties', size: 1, mtimeMs: 1 }], modified: [], removed: [] });
    expect(change).toEqual({ changed: [], failed: ['gone'] });
    expect(logger.text('error')).toContain('Unable to load properties file gone.properties');
    const removed = await manager.apply({ dir: 'x', added: [], modified: [], removed: [{ name: 'manual.properties', path: 'manual.properties', size: 1, mtimeMs: 1 }] });
    expect(removed.changed).toEqual([]);
    expect(registry.get('manual')).toEqual({ a: '1' });
  });

  it('tracks which deployments read which files', () => {
    const manager = new PropertiesManager();
    manager.track('orders.js', ['app', 'db']);
    manager.track('billing.js', ['db']);
    expect(manager.readersOf('db')).toEqual(['billing.js', 'orders.js']);
    expect(manager.affected(['app'])).toEqual(['orders.js']);
    expect(manager.affected(['db', 'app', 'none'])).toEqual(['billing.js', 'orders.js']);
    manager.track('orders.js', ['app']);
    expect(manager.affected(['db'])).toEqual(['billing.js']);
    manager.untrack('billing.js');
    expect(manager.affected(['db'])).toEqual([]);
  });

  it('finds config files from field and constructor metadata', () => {
    expect(getConfigFiles(ReadsApp).sort()).toEqual(['app', 'db']);
    expect(getConfigFiles(ReadsShared)).toEqual(['shared']);
    expect(getConfigFiles(class Plain {})).toEqual([]);
    expect(configFilesOf(inspectClasses([ReadsApp, ReadsShared]))).toEqual(['app', 'db', 'shared']);
  });
});
