import { defineConnectorConformance } from '@integratetech/conformance';
import { Header } from '@integratetech/core';
import { describe, expect, it } from 'vitest';
import { MemoryConnector } from '../src/index';

defineConnectorConformance('memory', (options) => new MemoryConnector(options));

describe('memory connector', () => {
  it('isolates stored messages from later mutation by the producer', async () => {
    const connector = new MemoryConnector({ pollIntervalMs: 5 });
    await connector.start();
    const properties = { key: 'original' };
    await connector.getQueueProducer('copy').send('x', { properties });
    properties.key = 'mutated';
    const message = await (await connector.getQueue('copy')).receive(200);
    expect(message!.properties.key).toBe('original');
    await connector.stop();
  });

  it('uses an injected clock for lease expiry', async () => {
    let now = 1_000;
    const connector = new MemoryConnector({ clock: () => now, leaseMs: 100, pollIntervalMs: 1 });
    await connector.start();
    await connector.getQueueProducer('clock').send('tick');
    const consumer = await connector.getQueue('clock');
    expect((await consumer.receive(0))!.headers[Header.DeliveryCount]).toBe(1);
    expect(await consumer.receive(0)).toBeNull();
    now += 101;
    expect((await consumer.receive(0))!.headers[Header.DeliveryCount]).toBe(2);
    await connector.stop();
  });
});
