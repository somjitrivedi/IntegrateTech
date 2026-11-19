import Database from 'better-sqlite3';
import {
  type BrokerStore,
  DEAD_LETTER_MAX_DELIVERIES,
  type DeadLetter,
  type InsertOptions,
  type LeaseRequest,
  type LeasedDelivery,
  type StoredMessage,
  sanitizeHeaders,
} from '@integratetech/core';

const MIGRATIONS: readonly string[] = [
  `
  CREATE TABLE messages (
    mid INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL,
    body TEXT NOT NULL,
    headers TEXT NOT NULL,
    properties TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    expires_at INTEGER
  );
  CREATE TABLE deliveries (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    mid INTEGER NOT NULL REFERENCES messages(mid),
    channel TEXT NOT NULL,
    priority INTEGER NOT NULL,
    available_at INTEGER NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    lease_owner TEXT,
    lease_token TEXT,
    lease_until INTEGER
  );
  CREATE INDEX deliveries_channel ON deliveries(channel, priority DESC, seq);
  CREATE INDEX deliveries_owner ON deliveries(lease_owner);
  CREATE INDEX deliveries_mid ON deliveries(mid);
  CREATE TABLE subscriptions (
    channel TEXT PRIMARY KEY,
    topic TEXT NOT NULL,
    durable INTEGER NOT NULL,
    created_at INTEGER NOT NULL
  );
  CREATE INDEX subscriptions_topic ON subscriptions(topic);
  CREATE TABLE dead_letters (
    seq INTEGER PRIMARY KEY,
    mid INTEGER NOT NULL REFERENCES messages(mid),
    channel TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    reason TEXT NOT NULL,
    dead_at INTEGER NOT NULL
  );
  CREATE INDEX dead_letters_channel ON dead_letters(channel);
  CREATE INDEX dead_letters_mid ON dead_letters(mid);
  `,
];

export const SCHEMA_VERSION = MIGRATIONS.length;

interface MessageRow {
  id: string;
  body: string;
  headers: string;
  properties: string;
  created_at: number;
  expires_at: number | null;
}

interface CandidateRow extends MessageRow {
  seq: number;
  mid: number;
  channel: string;
  attempts: number;
}

interface DeadRow extends MessageRow {
  seq: number;
  channel: string;
  attempts: number;
  reason: string;
  dead_at: number;
}

function toStored(row: MessageRow): StoredMessage {
  return {
    id: row.id,
    body: row.body,
    headers: sanitizeHeaders(JSON.parse(row.headers) as Record<string, unknown>),
    properties: JSON.parse(row.properties) as Record<string, string>,
    createdAt: row.created_at,
    expiresAt: row.expires_at,
  };
}

export function migrate(db: Database.Database): number {
  const current = db.pragma('user_version', { simple: true }) as number;
  for (let version = current; version < MIGRATIONS.length; version++) {
    db.transaction(() => {
      db.exec(MIGRATIONS[version]!);
      db.pragma(`user_version = ${version + 1}`);
    })();
  }
  return db.pragma('user_version', { simple: true }) as number;
}

export function openDatabase(filename: string): Database.Database {
  const db = new Database(filename);
  db.pragma('busy_timeout = 5000');
  if (filename !== ':memory:' && filename !== '') db.pragma('journal_mode = WAL');
  db.pragma('synchronous = NORMAL');
  db.pragma('foreign_keys = ON');
  return db;
}

export function migrateComponent(db: Database.Database, component: string, migrations: readonly string[]): number {
  return db
    .transaction(() => {
      db.exec('CREATE TABLE IF NOT EXISTS integrate_schema (component TEXT PRIMARY KEY, version INTEGER NOT NULL)');
      const row = db.prepare('SELECT version FROM integrate_schema WHERE component = ?').get(component) as { version: number } | undefined;
      let version = row?.version ?? 0;
      for (; version < migrations.length; version++) db.exec(migrations[version]!);
      db.prepare('INSERT INTO integrate_schema (component, version) VALUES (?, ?) ON CONFLICT(component) DO UPDATE SET version = excluded.version').run(
        component,
        version,
      );
      return version;
    })
    .immediate();
}

export class SqliteBrokerStore implements BrokerStore {
  private db?: Database.Database;
  private statements?: ReturnType<SqliteBrokerStore['prepare']>;

  constructor(readonly filename: string) {}

  get database(): Database.Database {
    if (!this.db) throw new Error('SQLite broker store is not open');
    return this.db;
  }

  async init(): Promise<void> {
    if (this.db) return;
    const db = openDatabase(this.filename);
    migrate(db);
    this.db = db;
    this.statements = this.prepare(db);
  }

  async close(): Promise<void> {
    this.db?.close();
    this.db = undefined;
    this.statements = undefined;
  }

  private get sql() {
    if (!this.statements) throw new Error('SQLite broker store is not open');
    return this.statements;
  }

  private prepare(db: Database.Database) {
    return {
      insertMessage: db.prepare(
        'INSERT INTO messages (id, body, headers, properties, created_at, expires_at) VALUES (@id, @body, @headers, @properties, @createdAt, @expiresAt)',
      ),
      insertDelivery: db.prepare('INSERT INTO deliveries (mid, channel, priority, available_at) VALUES (?, ?, ?, ?)'),
      subscribe: db.prepare('INSERT OR IGNORE INTO subscriptions (channel, topic, durable, created_at) VALUES (?, ?, ?, ?)'),
      subscription: db.prepare('SELECT durable FROM subscriptions WHERE channel = ?'),
      deleteSubscription: db.prepare('DELETE FROM subscriptions WHERE channel = ?'),
      channelMids: db.prepare('SELECT DISTINCT mid FROM deliveries WHERE channel = ?'),
      deleteChannel: db.prepare('DELETE FROM deliveries WHERE channel = ?'),
      channelsForTopic: db.prepare('SELECT channel FROM subscriptions WHERE topic = ? ORDER BY created_at, channel'),
      candidate: db.prepare(
        `SELECT d.seq, d.mid, d.channel, d.attempts, m.id, m.body, m.headers, m.properties, m.created_at, m.expires_at
         FROM deliveries d JOIN messages m ON m.mid = d.mid
         WHERE d.channel = ? AND d.available_at <= ? AND (d.lease_token IS NULL OR d.lease_until < ?)
         ORDER BY d.priority DESC, d.seq ASC LIMIT 1`,
      ),
      takeLease: db.prepare('UPDATE deliveries SET attempts = attempts + 1, lease_owner = ?, lease_token = ?, lease_until = ? WHERE seq = ?'),
      deleteDelivery: db.prepare('DELETE FROM deliveries WHERE seq = ?'),
      deadLetter: db.prepare('INSERT INTO dead_letters (seq, mid, channel, attempts, reason, dead_at) VALUES (?, ?, ?, ?, ?, ?)'),
      tokenDelivery: db.prepare('SELECT mid FROM deliveries WHERE seq = ? AND lease_token = ?'),
      release: db.prepare('UPDATE deliveries SET lease_owner = NULL, lease_token = NULL, lease_until = NULL WHERE seq = ? AND lease_token = ?'),
      releaseOwner: db.prepare('UPDATE deliveries SET lease_owner = NULL, lease_token = NULL, lease_until = NULL WHERE lease_owner = ?'),
      cleanupMessage: db.prepare(
        'DELETE FROM messages WHERE mid = ? AND NOT EXISTS (SELECT 1 FROM deliveries WHERE mid = ?) AND NOT EXISTS (SELECT 1 FROM dead_letters WHERE mid = ?)',
      ),
      depth: db.prepare('SELECT COUNT(*) AS count FROM deliveries WHERE channel = ?'),
      deadAll: db.prepare(
        `SELECT l.seq, l.channel, l.attempts, l.reason, l.dead_at, m.id, m.body, m.headers, m.properties, m.created_at, m.expires_at
         FROM dead_letters l JOIN messages m ON m.mid = l.mid ORDER BY l.dead_at, l.seq`,
      ),
      deadChannel: db.prepare(
        `SELECT l.seq, l.channel, l.attempts, l.reason, l.dead_at, m.id, m.body, m.headers, m.properties, m.created_at, m.expires_at
         FROM dead_letters l JOIN messages m ON m.mid = l.mid WHERE l.channel = ? ORDER BY l.dead_at, l.seq`,
      ),
    };
  }

  private cleanup(mid: number): void {
    this.sql.cleanupMessage.run(mid, mid, mid);
  }

  async insert(message: StoredMessage, channels: string[], options: InsertOptions): Promise<void> {
    const sql = this.sql;
    this.database.transaction(() => {
      const result = sql.insertMessage.run({
        id: message.id,
        body: message.body,
        headers: JSON.stringify(message.headers),
        properties: JSON.stringify(message.properties),
        createdAt: message.createdAt,
        expiresAt: message.expiresAt,
      });
      for (const channel of channels) sql.insertDelivery.run(result.lastInsertRowid, channel, options.priority, options.availableAt);
    })();
  }

  async subscribe(topic: string, channel: string, durable: boolean, now: number): Promise<void> {
    this.sql.subscribe.run(channel, topic, durable ? 1 : 0, now);
  }

  async unsubscribe(channel: string): Promise<void> {
    const sql = this.sql;
    this.database.transaction(() => {
      const row = sql.subscription.get(channel) as { durable: number } | undefined;
      if (!row || row.durable === 1) return;
      const mids = (sql.channelMids.all(channel) as { mid: number }[]).map((entry) => entry.mid);
      sql.deleteSubscription.run(channel);
      sql.deleteChannel.run(channel);
      for (const mid of mids) this.cleanup(mid);
    })();
  }

  async channelsForTopic(topic: string): Promise<string[]> {
    return (this.sql.channelsForTopic.all(topic) as { channel: string }[]).map((row) => row.channel);
  }

  async lease(request: LeaseRequest): Promise<LeasedDelivery | null> {
    const sql = this.sql;
    return this.database
      .transaction((): LeasedDelivery | null => {
        while (true) {
          const row = sql.candidate.get(request.channel, request.now, request.now) as CandidateRow | undefined;
          if (!row) return null;
          if (row.expires_at !== null && row.expires_at <= request.now) {
            sql.deleteDelivery.run(row.seq);
            this.cleanup(row.mid);
            continue;
          }
          if (row.attempts >= request.maxAttempts) {
            sql.deleteDelivery.run(row.seq);
            sql.deadLetter.run(row.seq, row.mid, row.channel, row.attempts, DEAD_LETTER_MAX_DELIVERIES, request.now);
            continue;
          }
          sql.takeLease.run(request.owner, request.token, request.now + request.leaseMs, row.seq);
          return { seq: row.seq, channel: row.channel, attempts: row.attempts + 1, message: toStored(row) };
        }
      })
      .immediate();
  }

  async ack(seq: number, token: string): Promise<boolean> {
    const sql = this.sql;
    return this.database
      .transaction(() => {
        const row = sql.tokenDelivery.get(seq, token) as { mid: number } | undefined;
        if (!row) return false;
        sql.deleteDelivery.run(seq);
        this.cleanup(row.mid);
        return true;
      })
      .immediate();
  }

  async release(seq: number, token: string): Promise<boolean> {
    return this.sql.release.run(seq, token).changes > 0;
  }

  async releaseOwner(owner: string): Promise<number> {
    if (!this.db) return 0;
    return this.sql.releaseOwner.run(owner).changes;
  }

  async depth(channel: string): Promise<number> {
    return (this.sql.depth.get(channel) as { count: number }).count;
  }

  async deadLetters(channel?: string): Promise<DeadLetter[]> {
    const rows = (channel === undefined ? this.sql.deadAll.all() : this.sql.deadChannel.all(channel)) as DeadRow[];
    return rows.map((row) => ({ seq: row.seq, channel: row.channel, attempts: row.attempts, reason: row.reason, deadAt: row.dead_at, message: toStored(row) }));
  }
}
