import { MemoryConnector } from '@integratetech/connector-memory';
import { type BrokerConnectorOptions, type Consumer, IntegrateTech, type Logger } from '../src/index';

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

export async function startIntegrate(options: BrokerConnectorOptions = {}) {
  const logger = recordingLogger();
  const connector = new MemoryConnector({ pollIntervalMs: 2, ...options });
  const integrate = new IntegrateTech({ logger });
  await integrate.start(connector);
  return { integrate, connector, logger };
}

export async function waitFor(condition: () => boolean | Promise<boolean>, timeoutMs = 3000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await condition())) {
    if (Date.now() > deadline) throw new Error('Timed out waiting for condition');
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

export async function runUntilIdle(consumer: Consumer): Promise<void> {
  consumer.setOutOfMessages((idle) => idle.shutdown());
  await consumer.listen();
}

export function deferred<T = void>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
