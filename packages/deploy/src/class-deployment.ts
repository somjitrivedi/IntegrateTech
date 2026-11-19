import { type IntegrateTech, describeError } from '@integratetech/core';
import { DeploymentBase, DeploymentError } from './deployment';
import { type DeploymentContents, type DrainResult, DeploymentRunner, type RunnerOptions, inspectClasses } from './runner';

export interface ClassDeploymentOptions extends RunnerOptions {
  drainTimeoutMs?: number;
}

export class ClassDeployment extends DeploymentBase {
  readonly kind = 'class' as const;
  private runner?: DeploymentRunner;
  private readonly contents: DeploymentContents;

  constructor(
    name: string,
    classes: Function[],
    readonly integrate: IntegrateTech,
    private readonly options: ClassDeploymentOptions = {},
  ) {
    super(name);
    this.contents = inspectClasses(classes);
    const probe = new DeploymentRunner(integrate, this.contents);
    this.consumers = probe.consumerNames();
    this.services = probe.serviceNames();
    this.files = probe.configFiles();
  }

  async start(): Promise<void> {
    if (this.status === 'running' || this.status === 'starting') return;
    this.status = 'starting';
    this.generation++;
    this.error = undefined;
    const runner = new DeploymentRunner(this.integrate, this.contents, this.options);
    try {
      await runner.start();
    } catch (error) {
      this.status = 'failed';
      this.error = describeError(error);
      throw new DeploymentError(`Unable to start deployment ${this.name}: ${this.error}`, { cause: error });
    }
    this.runner = runner;
    this.startedAt = Date.now();
    this.status = 'running';
  }

  processed(): number {
    return this.runner?.processed() ?? 0;
  }

  async stop(): Promise<DrainResult> {
    const runner = this.runner;
    this.runner = undefined;
    if (!runner) {
      if (this.status !== 'failed') this.status = 'stopped';
      return { drained: true };
    }
    this.status = 'stopping';
    const result = await runner.drain(this.options.drainTimeoutMs ?? 30_000);
    this.status = 'stopped';
    return result;
  }
}
