import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import Database from 'better-sqlite3';
import { defineConnectorConformance } from '@integratetech/conformance';
import { GroupedTopicSource, Header, type IntegrateMessage } from '@integratetech/core';
import { afterAll, afterEach, describe, expect, it } from 'vitest';
import { SCHEMA_VERSION, SqliteConnector, migrate } from '../src/index';

const root = mkdtempSync(join(tmpdir(), 'integratetech-sqlite-'));
let counter = 0;
const nextFile = () => join(root, `broker-${process.pid}-${++counter}.db`);

afterAll(() => {
  rmSync(root, { recursive: true, force: true });
});

defineConnectorConformance('sqlite (file)', (options) => new SqliteConnector({ ...options, filename: nextFile() }));
defineConnectorConformance('sqlite (memory)', (options) => new SqliteConnector({ ...options, filename: ':memory:' }));

describe('sqlite connector', () => {
  const open: SqliteConnector[] = [];

  async function connect(filename: string, options: { leaseMs?: number } = {}): Promise<SqliteConnector> {
    const connector = new SqliteConnector({ filename, pollIntervalMs: 5, ...options });
    await connector.start();
    open.push(connector);
    return connector;
  }

  afterEach(async () => {
    while (open.length > 0) await open.pop()!.stop();
  });

  it('runs migrations once and records the schema version', () => {
    const db = new Database(':memory:');
    expect(migrate(db)).toBe(SCHEMA_VERSION);
    expect(migrate(db)).toBe(SCHEMA_VERSION);
    const tables = (db.prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name").all() as { name: string }[]).map((row) => row.name);
    expect(tables).toEqual(expect.arrayContaining(['dead_letters', 'deliveries', 'messages', 'subscriptions']));
    db.close();
  });

  it('persists queued messages across restarts', async () => {
    const filename = nextFile();
    const writer = await connect(filename);
    const id = await writer.getQueueProducer('durable').send('survives', { properties: { k: 'v' } });
    await writer.stop();
    open.splice(open.indexOf(writer), 1);
    const reader = await connect(filename);
    const message = await (await reader.getQueue('durable')).receive(500);
    expect(message!.id).toBe(id);
    expect(message!.body).toBe('survives');
    expect(message!.properties).toEqual({ k: 'v' });
  });

  it('redelivers a message leased by a process that died before acknowledging', async () => {
    const filename = nextFile();
    const crashed = await connect(filename, { leaseMs: 150 });
    await crashed.getQueueProducer('crash').send('unfinished');
    const lost = await (await crashed.getQueue('crash')).receive(500);
    expect(lost).not.toBeNull();
    const survivor = await connect(filename, { leaseMs: 150 });
    const consumer = await survivor.getQueue('crash');
    expect(await consumer.receive(50)).toBeNull();
    const recovered = await consumer.receive(2000);
    expect(recovered!.id).toBe(lost!.id);
    expect(recovered!.headers[Header.DeliveryCount]).toBe(2);
    expect(await recovered!.acknowledge()).toBe(true);
    expect(await lost!.acknowledge()).toBe(false);
  });

  it('shares one database file between connectors without duplicate delivery', async () => {
    const filename = nextFile();
    const a = await connect(filename);
    const b = await connect(filename);
    const producer = a.getQueueProducer('shared');
    const sent = new Set<string>();
    for (let i = 0; i < 60; i++) sent.add(await producer.send(`n-${i}`));
    const seen: string[] = [];
    const take = async (connector: SqliteConnector) => {
      const consumer = await connector.getQueue('shared');
      let message: IntegrateMessage | null;
      while ((message = await consumer.receive(60))) {
        seen.push(message.id!);
        await message.acknowledge();
      }
    };
    await Promise.all([take(a), take(b)]);
    expect(seen).toHaveLength(60);
    expect(new Set(seen)).toEqual(sent);
  });

  it('keeps durable group subscriptions across restarts', async () => {
    const filename = nextFile();
    const first = await connect(filename);
    await (await first.getConsumerForSource(new GroupedTopicSource('orders', 'shipping'))).close();
    await first.stop();
    open.splice(open.indexOf(first), 1);
    const second = await connect(filename);
    await second.getTopicProducer('orders').send('order-1');
    const message = await (await second.getConsumerForSource(new GroupedTopicSource('orders', 'shipping'))).receive(500);
    expect(message!.body).toBe('order-1');
  });

  it('removes message rows once every delivery is acknowledged', async () => {
    const connector = await connect(nextFile());
    const a = await connector.getTopic('cleanup');
    const b = await connector.getTopic('cleanup');
    await connector.getTopicProducer('cleanup').send('both');
    const count = () => (connector.store.database.prepare('SELECT COUNT(*) AS c FROM messages').get() as { c: number }).c;
    await (await a.receive(500))!.acknowledge();
    expect(count()).toBe(1);
    await (await b.receive(500))!.acknowledge();
    expect(count()).toBe(0);
  });

  it('cleans pending deliveries of a closed non-durable topic subscriber', async () => {
    const connector = await connect(nextFile());
    const subscriber = await connector.getTopic('transient');
    await connector.getTopicProducer('transient').send('never-read');
    await subscriber.close();
    const rows = (connector.store.database.prepare('SELECT COUNT(*) AS c FROM deliveries').get() as { c: number }).c;
    const messages = (connector.store.database.prepare('SELECT COUNT(*) AS c FROM messages').get() as { c: number }).c;
    expect(rows).toBe(0);
    expect(messages).toBe(0);
  });

  it('wraps store failures in a connector error on start', async () => {
    const connector = new SqliteConnector({ filename: join(root, 'missing-dir', 'nested', 'x.db') });
    await expect(connector.start()).rejects.toThrow('Unable to start broker store');
  });
});
