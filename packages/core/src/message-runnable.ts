import type { Consumer } from './consumer';
import { describeError } from './logger';
import { IntegrateMessage } from './message';
import type { MemberKey } from './metadata';
import { ProcessStep } from './process-step';

export const RESPONSE_URI = '@ResponseUri';

export interface Closeable {
  close(): Promise<void> | void;
}

export type ProcessOutcome = ProcessStep.Complete | ProcessStep.Invalid | ProcessStep.Error | 'Skipped';

function describeType(value: unknown): string {
  if (value === null) return 'null';
  if (Array.isArray(value)) return 'array';
  return typeof value;
}

export function parseResponseUri(uri: string): { scheme: string; target: string } | null {
  const match = /^([a-zA-Z][a-zA-Z0-9+.-]*):\/\/(.+)$/.exec(uri.trim());
  if (!match) return null;
  return { scheme: match[1]!.toLowerCase(), target: match[2]! };
}

export class MessageRunnable {
  readonly errors: string[] = [];
  warnOnly = false;
  outcome?: ProcessOutcome;

  constructor(
    readonly consumer: Consumer,
    readonly msg: IntegrateMessage,
    readonly handler: any,
    private readonly closeMe: Closeable[],
  ) {}

  getErrors(): string[] {
    return this.errors;
  }

  addError(error: string): void {
    this.errors.push(error);
  }

  addErrors(errors: unknown[] | null | undefined): void {
    if (!errors) return;
    for (const error of errors) this.errors.push(String(error));
  }

  setWarnOnly(warnOnly: boolean): void {
    this.warnOnly = warnOnly;
  }

  private get lifeCycle() {
    return this.consumer.integrate.lifeCycle;
  }

  private get logger() {
    return this.consumer.integrate.logger;
  }

  async pending(): Promise<void> {
    await this.lifeCycle.call(ProcessStep.Pending, this);
  }

  private async attempt<T>(work: () => T | Promise<T>): Promise<{ ok: true; value: T } | { ok: false }> {
    try {
      return { ok: true, value: await work() };
    } catch (error) {
      this.addError(describeError(error));
      return { ok: false };
    }
  }

  private async enter(step: ProcessStep): Promise<void> {
    for (const method of this.consumer.hooksFor(step)) await this.attempt(() => this.handler[method]());
    await this.lifeCycle.call(step, this);
  }

  private async validate(methods: MemberKey[]): Promise<void> {
    for (const method of methods) {
      const result = await this.attempt(() => this.handler[method]());
      if (!result.ok) continue;
      const value = result.value;
      if (Array.isArray(value)) this.addErrors(value);
      else if (typeof value === 'boolean') {
        if (!value) this.addError(`Validator ${String(method)} returned false`);
      } else this.addError(`onValidate method ${String(method)} has wrong return type ${describeType(value)}`);
    }
  }

  private async respond(): Promise<void> {
    const uriText = this.msg.properties?.[RESPONSE_URI];
    if (uriText === undefined) return;
    const uri = parseResponseUri(uriText);
    if (!uri) {
      this.logger.error(`Unable to determine response uri from ${uriText}`);
      return;
    }
    const connector = this.consumer.integrate.getConnector();
    for (const method of this.consumer.meta.onResponse) {
      await this.attempt(async () => {
        const value = await this.handler[method]();
        const response = new IntegrateMessage(typeof value === 'string' ? value : JSON.stringify(value ?? null));
        response.setHeaders(this.msg.headers);
        if (uri.scheme === 'queue' || uri.scheme === 'topic') {
          const producer = uri.scheme === 'queue' ? connector.getQueueProducer(uri.target) : connector.getTopicProducer(uri.target);
          try {
            await producer.send(response);
          } finally {
            await producer.close();
          }
        } else if (uri.scheme !== 'http' && uri.scheme !== 'https') {
          this.logger.warn(`Unable to find mapping for response uri scheme ${uri.scheme}`);
        }
      });
    }
  }

  private async close(): Promise<void> {
    for (const closeable of this.closeMe) {
      try {
        await closeable.close();
      } catch (error) {
        this.logger.error(`Unable to close a resource: ${describeError(error)}`);
      }
    }
  }

  async run(): Promise<ProcessOutcome> {
    try {
      let proceed: boolean;
      try {
        proceed = await this.msg.beforeProcessing();
      } catch (error) {
        proceed = false;
        this.logger.error(`Error before processing message: ${describeError(error)}`);
      }
      if (!proceed) {
        this.outcome = 'Skipped';
        return this.outcome;
      }
      await this.enter(ProcessStep.Validating);
      await this.validate(this.consumer.meta.onValidate);
      if (this.errors.length > 0) {
        this.outcome = ProcessStep.Invalid;
        await this.enter(ProcessStep.Invalid);
      } else {
        await this.enter(ProcessStep.Processing);
        for (const method of this.consumer.meta.onMessage) await this.attempt(() => this.handler[method]());
        if (this.errors.length > 0) {
          this.outcome = ProcessStep.Error;
          await this.enter(ProcessStep.Error);
        } else {
          this.outcome = ProcessStep.Complete;
          for (const method of this.consumer.hooksFor(ProcessStep.Complete)) await this.attempt(() => this.handler[method]());
          await this.respond();
          await this.lifeCycle.call(ProcessStep.Complete, this);
        }
      }
      try {
        if (!(await this.msg.acknowledge())) this.logger.error('Failed to acknowledge message after processing');
      } catch (error) {
        this.logger.error(`Error while acking message: ${describeError(error)}`);
      }
      if (this.errors.length > 0) {
        const text = `Errors processing message ${this.msg.id} on ${this.consumer.name}: ${this.errors.join('; ')}`;
        if (this.warnOnly) this.logger.warn(text);
        else this.logger.error(text);
      }
      return this.outcome;
    } catch (error) {
      this.logger.error(`Unexpected failure running message ${this.msg.id}: ${describeError(error)}`);
      this.outcome ??= ProcessStep.Error;
      return this.outcome;
    } finally {
      await this.close();
    }
  }
}
