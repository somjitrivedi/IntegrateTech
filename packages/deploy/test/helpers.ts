import { mkdirSync, mkdtempSync, renameSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { SqliteConnector } from '@integratetech/connector-sqlite';
import type { IntegrateMessage, Logger, MessageConsumer } from '@integratetech/core';
import ts from 'typescript';
import type { ModuleSpec } from '../src/index';

const WORK_ROOT = join(dirname(fileURLToPath(import.meta.url)), '.work');

export function workDir(prefix: string): { root: string; deployDir: string; propertiesDir: string; db: string; cleanup: () => void } {
  mkdirSync(WORK_ROOT, { recursive: true });
  const root = mkdtempSync(join(WORK_ROOT, `${prefix}-`));
  const deployDir = join(root, 'deploy');
  const propertiesDir = join(root, 'properties');
  mkdirSync(deployDir);
  mkdirSync(propertiesDir);
  return { root, deployDir, propertiesDir, db: join(root, 'broker.db'), cleanup: () => rmSync(root, { recursive: true, force: true }) };
}

export function compile(source: string): string {
  return ts.transpileModule(source, {
    compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022, experimentalDecorators: true, useDefineForClassFields: false },
  }).outputText;
}

export function writeAtomic(path: string, content: string): void {
  const temp = `${path}.tmp-${process.pid}-${Date.now()}`;
  writeFileSync(temp, content);
  renameSync(temp, path);
}

export function writeBundle(dir: string, name: string, source: string): string {
  const path = join(dir, name);
  writeAtomic(path, compile(source));
  return path;
}

export function sqliteSpec(filename: string, options: Record<string, unknown> = {}): ModuleSpec {
  return { module: '@integratetech/connector-sqlite', export: 'SqliteConnector', options: { filename, pollIntervalMs: 5, ...options } };
}

export async function openBroker(filename: string): Promise<SqliteConnector> {
  const connector = new SqliteConnector({ filename, pollIntervalMs: 5 });
  await connector.start();
  return connector;
}

export class EventReader {
  readonly events: Record<string, unknown>[] = [];
  private consumer?: MessageConsumer;

  constructor(
    private readonly connector: SqliteConnector,
    private readonly queue = 'events',
  ) {}

  async drain(waitMs = 20): Promise<Record<string, unknown>[]> {
    this.consumer ??= await this.connector.getQueue(this.queue);
    while (true) {
      const message: IntegrateMessage | null = await this.consumer.receive(waitMs);
      if (!message) return this.events;
      this.events.push(JSON.parse(message.body ?? 'null') as Record<string, unknown>);
      await message.acknowledge();
    }
  }

  async waitFor(predicate: (events: Record<string, unknown>[]) => boolean, timeoutMs = 15_000): Promise<Record<string, unknown>[]> {
    const deadline = Date.now() + timeoutMs;
    while (true) {
      await this.drain();
      if (predicate(this.events)) return this.events;
      if (Date.now() > deadline) throw new Error(`Timed out waiting for events; saw ${JSON.stringify(this.events)}`);
    }
  }

  async close(): Promise<void> {
    await this.consumer?.close();
  }
}

export interface LogEntry {
  level: 'debug' | 'info' | 'warn' | 'error';
  message: string;
}

export function recordingLogger(): Logger & { entries: LogEntry[]; text(level?: LogEntry['level']): string } {
  const entries: LogEntry[] = [];
  const add = (level: LogEntry['level']) => (message: string) => {
    entries.push({ level, message });
  };
  return {
    entries,
    debug: add('debug'),
    info: add('info'),
    warn: add('warn'),
    error: add('error'),
    text: (level) =>
      entries
        .filter((entry) => !level || entry.level === level)
        .map((entry) => entry.message)
        .join('\n'),
  };
}

export async function waitFor(condition: () => boolean | Promise<boolean>, timeoutMs = 15_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await condition())) {
    if (Date.now() > deadline) throw new Error('Timed out waiting for condition');
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

export const producerBundle = (version: string, queue = 'orders') => `
import { Message, OnMessage, Producer, Queue, type MessageProducer } from '@integratetech/core';

@Queue('${queue}')
export class OrderConsumer {
  @Message() body!: string;
  @Producer({ queue: 'events' }) events!: MessageProducer;

  @OnMessage()
  async handle() {
    await this.events.send(JSON.stringify({ version: '${version}', queue: '${queue}', body: this.body }));
  }
}
`;
