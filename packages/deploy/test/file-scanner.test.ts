import { appendFileSync, rmSync, utimesSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { DeploymentWatch, FileScanner, type ScanResult, extensionFilter, isEmpty } from '../src/index';
import { recordingLogger, workDir } from './helpers';

const names = (result: ScanResult) => ({
  added: result.added.map((file) => file.name),
  modified: result.modified.map((file) => file.name),
  removed: result.removed.map((file) => file.name),
});

let cleanup: (() => void) | undefined;

afterEach(() => {
  cleanup?.();
  cleanup = undefined;
});

function setup() {
  const dir = workDir('scan');
  cleanup = dir.cleanup;
  return dir.deployDir;
}

function touch(path: string, seconds: number): void {
  utimesSync(path, seconds, seconds);
}

describe('FileScanner', () => {
  it('reports a file only after two scans see it unchanged', async () => {
    const dir = setup();
    const scanner = new FileScanner(dir, { filter: extensionFilter('.js') });
    writeFileSync(join(dir, 'a.js'), 'one');
    expect(names(await scanner.scan())).toEqual({ added: [], modified: [], removed: [] });
    expect(names(await scanner.scan())).toEqual({ added: ['a.js'], modified: [], removed: [] });
    expect(isEmpty(await scanner.scan())).toBe(true);
    expect(scanner.files().map((file) => file.name)).toEqual(['a.js']);
  });

  it('skips a file that is still being written', async () => {
    const dir = setup();
    const scanner = new FileScanner(dir);
    const path = join(dir, 'big.js');
    writeFileSync(path, 'part-1');
    await scanner.scan();
    appendFileSync(path, 'part-2');
    expect(isEmpty(await scanner.scan())).toBe(true);
    appendFileSync(path, 'part-3');
    expect(isEmpty(await scanner.scan())).toBe(true);
    const done = await scanner.scan();
    expect(done.added.map((file) => file.size)).toEqual([18]);
  });

  it('reports modified and removed files', async () => {
    const dir = setup();
    const scanner = new FileScanner(dir);
    const a = join(dir, 'a.js');
    const b = join(dir, 'b.js');
    writeFileSync(a, 'a');
    writeFileSync(b, 'b');
    await scanner.scan();
    await scanner.scan();
    writeFileSync(a, 'a2');
    touch(a, 1_700_000_000);
    rmSync(b);
    expect(names(await scanner.scan())).toEqual({ added: [], modified: [], removed: ['b.js'] });
    expect(names(await scanner.scan())).toEqual({ added: [], modified: ['a.js'], removed: [] });
  });

  it('detects a change in modification time alone', async () => {
    const dir = setup();
    const scanner = new FileScanner(dir);
    const a = join(dir, 'a.js');
    writeFileSync(a, 'same');
    touch(a, 1_600_000_000);
    await scanner.scan();
    await scanner.scan();
    touch(a, 1_650_000_000);
    await scanner.scan();
    expect(names(await scanner.scan()).modified).toEqual(['a.js']);
  });

  it('forgets a pending file that disappears before it settles', async () => {
    const dir = setup();
    const scanner = new FileScanner(dir);
    const a = join(dir, 'a.js');
    writeFileSync(a, 'a');
    await scanner.scan();
    rmSync(a);
    expect(isEmpty(await scanner.scan())).toBe(true);
    writeFileSync(a, 'a');
    expect(isEmpty(await scanner.scan())).toBe(true);
  });

  it('filters by extension and ignores directories and a missing folder', async () => {
    const dir = setup();
    writeFileSync(join(dir, 'keep.JS'), 'x');
    writeFileSync(join(dir, 'skip.txt'), 'x');
    const scanner = new FileScanner(dir, { filter: extensionFilter('.js', '.mjs') });
    await scanner.scan();
    expect(names(await scanner.scan()).added).toEqual(['keep.JS']);
    const missing = new FileScanner(join(dir, 'nope'));
    expect(isEmpty(await missing.scan())).toBe(true);
  });
});

describe('DeploymentWatch', () => {
  it('scans twice on start so existing files deploy immediately', async () => {
    const dir = setup();
    writeFileSync(join(dir, 'a.js'), 'a');
    const seen: string[][] = [];
    const watch = new DeploymentWatch([new FileScanner(dir)], (results) => {
      seen.push(results.flatMap((result) => result.added.map((file) => file.name)));
    }, { intervalMs: 60_000, settleMs: 5 });
    await watch.start();
    expect(seen).toEqual([[], ['a.js']]);
    expect(watch.scanCount()).toBe(2);
    await watch.stop();
    expect(watch.isRunning()).toBe(false);
  });

  it('polls on its interval and never runs two scans at once', async () => {
    const dir = setup();
    let active = 0;
    let maxActive = 0;
    let calls = 0;
    const watch = new DeploymentWatch(
      [new FileScanner(dir)],
      async () => {
        active++;
        calls++;
        maxActive = Math.max(maxActive, active);
        await new Promise((resolve) => setTimeout(resolve, 15));
        active--;
      },
      { intervalMs: 5, settleMs: 0 },
    );
    await watch.start();
    await Promise.all([watch.scanNow(), watch.scanNow(), watch.scanNow()]);
    await new Promise((resolve) => setTimeout(resolve, 80));
    await watch.stop();
    const after = calls;
    await new Promise((resolve) => setTimeout(resolve, 40));
    expect(maxActive).toBe(1);
    expect(calls).toBeGreaterThan(3);
    expect(calls).toBe(after);
  });

  it('logs handler failures and keeps polling', async () => {
    const dir = setup();
    const logger = recordingLogger();
    let calls = 0;
    const watch = new DeploymentWatch(
      [new FileScanner(dir)],
      () => {
        calls++;
        throw new Error('handler broke');
      },
      { intervalMs: 5, settleMs: 0, logger },
    );
    await watch.start();
    await new Promise((resolve) => setTimeout(resolve, 40));
    await watch.stop();
    expect(calls).toBeGreaterThan(2);
    expect(logger.text('error')).toContain('Deployment scan failed: handler broke');
  });

  it('stops during the settle delay without hanging', async () => {
    const dir = setup();
    const watch = new DeploymentWatch([new FileScanner(dir)], () => {}, { intervalMs: 5, settleMs: 60_000 });
    const started = watch.start();
    await new Promise((resolve) => setTimeout(resolve, 10));
    await watch.stop();
    await started;
    expect(watch.scanCount()).toBe(1);
  });
});
