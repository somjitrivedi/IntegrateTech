import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    projects: [
      { extends: true, test: { name: 'core', include: ['packages/core/test/**/*.test.ts'] } },
      { extends: true, test: { name: 'connector-memory', include: ['packages/connector-memory/test/**/*.test.ts'] } },
      { extends: true, test: { name: 'connector-sqlite', include: ['packages/connector-sqlite/test/**/*.test.ts'] } },
      { extends: true, test: { name: 'deploy', include: ['packages/deploy/test/**/*.test.ts'] } },
    ],
    testTimeout: 15000,
  },
});
