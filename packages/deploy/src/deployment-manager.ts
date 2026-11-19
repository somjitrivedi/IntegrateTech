import { basename } from 'node:path';
import { type IntegrateTech, type Logger, type PropertiesRegistry, describeError, silentLogger } from '@integratetech/core';
import { ClassDeployment } from './class-deployment';
import type { Deployment, DeploymentInfo } from './deployment';
import { DeploymentWatch } from './deployment-watch';
import { FileScanner, type ScanResult, extensionFilter } from './file-scanner';
import { PROPERTIES_EXTENSION, PropertiesManager } from './properties-manager';
import type { ModuleSpec } from './protocol';
import type { RunnerOptions } from './runner';
import { WorkerDeployment } from './worker-deployment';

export const DEFAULT_BUNDLE_EXTENSIONS: readonly string[] = ['.js', '.mjs'];

export interface DeploymentManagerOptions {
  deployDir?: string;
  propertiesDir?: string;
  connector?: ModuleSpec;
  plugins?: ModuleSpec[];
  integrate?: IntegrateTech;
  properties?: PropertiesRegistry;
  pollIntervalMs?: number;
  settleMs?: number;
  drainTimeoutMs?: number;
  startTimeoutMs?: number;
  runner?: RunnerOptions;
  bundleExtensions?: readonly string[];
  bootstrap?: URL | string;
  logger?: Logger;
}

export type DeploymentEvent =
  | { type: 'deployed'; name: string; info: DeploymentInfo }
  | { type: 'redeployed'; name: string; reason: 'bundle' | 'properties'; drained: boolean; info: DeploymentInfo }
  | { type: 'undeployed'; name: string; drained: boolean }
  | { type: 'failed'; name: string; error: string }
  | { type: 'properties'; changed: string[]; affected: string[] };

export type DeploymentListener = (event: DeploymentEvent) => void;

export class DeploymentManager {
  readonly propertiesManager: PropertiesManager;
  private readonly logger: Logger;
  private readonly deployments = new Map<string, Deployment>();
  private readonly chains = new Map<string, Promise<void>>();
  private readonly listeners = new Set<DeploymentListener>();
  private readonly propertiesScanner?: FileScanner;
  private readonly bundleScanner?: FileScanner;
  private watch?: DeploymentWatch;
  private running = false;

  constructor(private readonly options: DeploymentManagerOptions = {}) {
    if (options.deployDir && !options.connector) throw new Error('A connector spec is required to deploy bundles from a directory');
    this.logger = options.logger ?? options.integrate?.logger ?? silentLogger;
    const registry = options.integrate?.properties ?? options.properties;
    this.propertiesManager = new PropertiesManager({ logger: this.logger, ...(registry ? { registry } : {}) });
    if (options.propertiesDir) this.propertiesScanner = new FileScanner(options.propertiesDir, { filter: extensionFilter(PROPERTIES_EXTENSION) });
    if (options.deployDir) this.bundleScanner = new FileScanner(options.deployDir, { filter: extensionFilter(...(options.bundleExtensions ?? DEFAULT_BUNDLE_EXTENSIONS)) });
  }

  on(listener: DeploymentListener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  private emit(event: DeploymentEvent): void {
    for (const listener of this.listeners) {
      try {
        listener(event);
      } catch (error) {
        this.logger.error(`Deployment listener failed: ${describeError(error)}`);
      }
    }
  }

  isRunning(): boolean {
    return this.running;
  }

  list(): DeploymentInfo[] {
    return [...this.deployments.values()].map((deployment) => deployment.info()).sort((a, b) => a.name.localeCompare(b.name));
  }

  get(name: string): Deployment | undefined {
    return this.deployments.get(name);
  }

  async start(): Promise<void> {
    if (this.running) return;
    this.running = true;
    const scanners = [this.propertiesScanner, this.bundleScanner].filter((scanner): scanner is FileScanner => scanner !== undefined);
    if (scanners.length === 0) return;
    this.watch = new DeploymentWatch(scanners, (results) => this.apply(results), {
      logger: this.logger,
      ...(this.options.pollIntervalMs !== undefined ? { intervalMs: this.options.pollIntervalMs } : {}),
      ...(this.options.settleMs !== undefined ? { settleMs: this.options.settleMs } : {}),
    });
    await this.watch.start();
  }

  scan(): Promise<void> {
    if (!this.watch) return Promise.resolve();
    return this.watch.scanNow();
  }

  async stop(): Promise<void> {
    this.running = false;
    await this.watch?.stop();
    this.watch = undefined;
    await Promise.all([...this.deployments.keys()].map((name) => this.undeploy(name)));
  }

  async registerDeployment(name: string, ...classes: Function[]): Promise<DeploymentInfo> {
    const integrate = this.options.integrate;
    if (!integrate) throw new Error('Class deployments need an IntegrateTech instance');
    return this.enqueue(name, async () => {
      const previous = await this.retire(name);
      const deployment = new ClassDeployment(name, classes, integrate, {
        ...this.options.runner,
        ...(this.options.drainTimeoutMs !== undefined ? { drainTimeoutMs: this.options.drainTimeoutMs } : {}),
      });
      this.deployments.set(name, deployment);
      await this.launch(deployment, previous === undefined ? undefined : { reason: 'bundle', drained: previous });
      return deployment.info();
    });
  }

  unregisterDeployment(name: string): Promise<void> {
    return this.undeploy(name);
  }

  private enqueue<T>(name: string, operation: () => Promise<T>): Promise<T> {
    const previous = this.chains.get(name) ?? Promise.resolve();
    const result = previous.then(operation);
    const settled = result.then(
      () => undefined,
      () => undefined,
    );
    this.chains.set(name, settled);
    void settled.then(() => {
      if (this.chains.get(name) === settled) this.chains.delete(name);
    });
    return result;
  }

  private async retire(name: string): Promise<boolean | undefined> {
    const existing = this.deployments.get(name);
    if (!existing) return undefined;
    const { drained } = await existing.stop();
    this.propertiesManager.untrack(name);
    this.deployments.delete(name);
    return drained;
  }

  private async launch(deployment: Deployment, redeploy?: { reason: 'bundle' | 'properties'; drained: boolean }): Promise<void> {
    try {
      await deployment.start(this.propertiesManager.snapshot());
      this.logger.info(`Deployed ${deployment.name}`);
      if (redeploy) this.emit({ type: 'redeployed', name: deployment.name, ...redeploy, info: deployment.info() });
      else this.emit({ type: 'deployed', name: deployment.name, info: deployment.info() });
    } catch (error) {
      const message = describeError(error);
      this.logger.error(`Deployment ${deployment.name} failed: ${message}`);
      this.emit({ type: 'failed', name: deployment.name, error: message });
    } finally {
      this.propertiesManager.track(deployment.name, deployment.configFiles());
    }
  }

  private deployFile(path: string): Promise<void> {
    const name = basename(path);
    return this.enqueue(name, async () => {
      const previous = await this.retire(name);
      const deployment = new WorkerDeployment(
        path,
        {
          connector: this.options.connector!,
          logger: this.logger,
          ...(this.options.plugins ? { plugins: this.options.plugins } : {}),
          ...(this.options.runner ? { runner: this.options.runner } : {}),
          ...(this.options.startTimeoutMs !== undefined ? { startTimeoutMs: this.options.startTimeoutMs } : {}),
          ...(this.options.drainTimeoutMs !== undefined ? { drainTimeoutMs: this.options.drainTimeoutMs } : {}),
          ...(this.options.bootstrap ? { bootstrap: this.options.bootstrap } : {}),
          onFailure: (failed, error) => this.emit({ type: 'failed', name: failed.name, error }),
        },
        name,
      );
      this.deployments.set(name, deployment);
      await this.launch(deployment, previous === undefined ? undefined : { reason: 'bundle', drained: previous });
    });
  }

  private undeploy(name: string): Promise<void> {
    return this.enqueue(name, async () => {
      const drained = await this.retire(name);
      if (drained === undefined) return;
      this.logger.info(`Undeployed ${name}`);
      this.emit({ type: 'undeployed', name, drained });
    });
  }

  private restart(name: string): Promise<void> {
    return this.enqueue(name, async () => {
      const deployment = this.deployments.get(name);
      if (!deployment) return;
      const { drained } = await deployment.stop();
      await this.launch(deployment, { reason: 'properties', drained });
    });
  }

  private async apply(results: ScanResult[]): Promise<void> {
    const properties = results.find((result) => result.dir === this.propertiesScanner?.dir);
    const bundles = results.find((result) => result.dir === this.bundleScanner?.dir);
    const touched = new Set<string>();
    for (const file of [...(bundles?.added ?? []), ...(bundles?.modified ?? []), ...(bundles?.removed ?? [])]) touched.add(file.name);
    if (properties) {
      const change = await this.propertiesManager.apply(properties);
      if (change.changed.length > 0) {
        const affected = this.propertiesManager.affected(change.changed).filter((name) => !touched.has(name));
        this.emit({ type: 'properties', changed: change.changed, affected });
        await Promise.all(affected.map((name) => this.restart(name)));
      }
    }
    if (!bundles) return;
    await Promise.all([
      ...bundles.removed.map((file) => this.undeploy(file.name)),
      ...[...bundles.added, ...bundles.modified].map((file) => this.deployFile(file.path)),
    ]);
  }
}
