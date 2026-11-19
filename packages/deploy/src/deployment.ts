import type { PropertiesSnapshot } from './properties-manager';
import type { DrainResult } from './runner';

export type DeploymentKind = 'file' | 'class';

export type DeploymentStatus = 'idle' | 'starting' | 'running' | 'stopping' | 'stopped' | 'failed';

export interface DeploymentInfo {
  name: string;
  kind: DeploymentKind;
  status: DeploymentStatus;
  file?: string;
  consumers: string[];
  services: string[];
  configFiles: string[];
  generation: number;
  startedAt?: number;
  error?: string;
}

export class DeploymentError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = 'DeploymentError';
  }
}

export interface Deployment {
  readonly name: string;
  readonly kind: DeploymentKind;
  info(): DeploymentInfo;
  configFiles(): string[];
  start(properties: PropertiesSnapshot): Promise<void>;
  stop(): Promise<DrainResult>;
}

export abstract class DeploymentBase implements Deployment {
  abstract readonly kind: DeploymentKind;
  protected status: DeploymentStatus = 'idle';
  protected consumers: string[] = [];
  protected services: string[] = [];
  protected files: string[] = [];
  protected generation = 0;
  protected startedAt?: number;
  protected error?: string;

  constructor(readonly name: string) {}

  getStatus(): DeploymentStatus {
    return this.status;
  }

  configFiles(): string[] {
    return [...this.files];
  }

  protected file(): string | undefined {
    return undefined;
  }

  info(): DeploymentInfo {
    const file = this.file();
    return {
      name: this.name,
      kind: this.kind,
      status: this.status,
      ...(file ? { file } : {}),
      consumers: [...this.consumers],
      services: [...this.services],
      configFiles: [...this.files],
      generation: this.generation,
      ...(this.startedAt !== undefined ? { startedAt: this.startedAt } : {}),
      ...(this.error !== undefined ? { error: this.error } : {}),
    };
  }

  abstract start(properties: PropertiesSnapshot): Promise<void>;
  abstract stop(): Promise<DrainResult>;
}
