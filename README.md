# IntegrateTech

A TypeScript + SQLite rebuild of the Forklift enterprise service bus. You write decorated consumer classes,
and IntegrateTech runs every message through a fixed lifecycle against a SQLite-backed (or in-memory) broker.
See [PHASES.md](PHASES.md) for the full plan. Phases 0–4 are built.

## Packages

| Package | Contents |
|---|---|
| `@integratetech/core` | Message model, headers, sources, decorators, consumer runtime, lifecycle monitors, roles, request/response, shared broker connector |
| `@integratetech/connector-sqlite` | `SqliteConnector`, a SQLite broker with leases, redelivery, dead letters, delayed delivery and expiration |
| `@integratetech/connector-memory` | `MemoryConnector`, the same semantics held in memory, for tests and embedding |
| `@integratetech/conformance` | Shared test suite that every connector must pass |
| `@integratetech/deploy` | Hot deployment: directory polling, `.properties` hot reload, one worker thread per `.js` bundle, drain-then-swap redeploys, in-process `ClassDeployment` |

## Commands

```
npm install
npm run typecheck
npm run lint
npm test
npm run test:core
npm run test:memory
npm run test:sqlite
npm run test:deploy
```

On Windows, run the tests from PowerShell or cmd. Under Git Bash, vitest 5.1 can't find its test suites.

Node 22+ is required. On machines where npm skips install scripts, the `allowScripts` entry in `package.json` lets the better-sqlite3 binding install.

## Example

```ts
import { IntegrateTech, Queue, OnValidate, OnMessage, Message, Property, ProcessStep, On } from '@integratetech/core';
import { SqliteConnector } from '@integratetech/connector-sqlite';

class Order {
  id?: string;
  total?: number;
}

@Queue('orders')
class OrderConsumer {
  @Message(Order) order!: Order;
  @Property('region') region!: string;

  @OnValidate()
  validate() {
    return this.order.total && this.order.total > 0 ? [] : ['total must be positive'];
  }

  @OnMessage()
  async handle() {}

  @On(ProcessStep.Error, ProcessStep.Invalid)
  alert() {}
}

const integrate = new IntegrateTech();
await integrate.start(new SqliteConnector({ filename: 'data/broker.db' }));
const threads = integrate.startConsumers(OrderConsumer);
await integrate.getConnector().getQueueProducer('orders').send({ id: 'o-1', total: 12 }, { properties: { region: 'eu' } });
```

## Hot deployment

```ts
import { DeploymentManager } from '@integratetech/deploy';

const manager = new DeploymentManager({
  deployDir: 'deploy',
  propertiesDir: 'properties',
  connector: { module: '@integratetech/connector-sqlite', export: 'SqliteConnector', options: { filename: 'data/broker.db' } },
});
manager.on((event) => console.log(event.type, 'name' in event ? event.name : ''));
await manager.start();
```

Drop a compiled `.js` bundle that exports decorated consumer classes into `deploy/`. Each bundle runs in its own worker thread. Editing it drains in-flight messages before the new version starts, and deleting it undeploys it. A change to `properties/<name>.properties` restarts every deployment that reads `@Config('<name>')`.
