import { BrokerConnector, type BrokerConnectorOptions } from '@integratetech/core';
import { SqliteBrokerStore } from './store';

export { SCHEMA_VERSION, SqliteBrokerStore, migrate, migrateComponent, openDatabase } from './store';

export interface SqliteConnectorOptions extends BrokerConnectorOptions {
  filename: string;
}

export class SqliteConnector extends BrokerConnector {
  declare readonly store: SqliteBrokerStore;

  constructor(options: SqliteConnectorOptions) {
    const { filename, ...rest } = options;
    super(new SqliteBrokerStore(filename), rest);
  }
}
