import { type Logger, describeError, silentLogger } from './logger';
import { type MemberKey, readMeta } from './metadata';
import type { MessageRunnable } from './message-runnable';
import type { ProcessStep } from './process-step';

interface Monitor {
  owner: Function;
  method: MemberKey;
  receiver: any;
  annotation?: { key: symbol; annotationName: string };
}

export class LifeCycleMonitors {
  private readonly monitors = new Map<ProcessStep, Monitor[]>();
  private calls = 0;

  constructor(private readonly logger: Logger = silentLogger) {}

  register(target: Function | object): boolean {
    const owner = typeof target === 'function' ? target : (target as { constructor: Function }).constructor;
    const bindings = readMeta(owner)?.lifecycle ?? [];
    if (bindings.length === 0) {
      this.logger.warn(`No @LifeCycle methods found on ${owner.name}`);
      return false;
    }
    let instance: any = typeof target === 'function' ? undefined : target;
    if (instance === undefined && bindings.some((binding) => !binding.isStatic)) {
      try {
        instance = new (owner as new () => object)();
      } catch (error) {
        this.logger.error(`Unable to instantiate lifecycle monitor ${owner.name}: ${describeError(error)}`);
        return false;
      }
    }
    for (const binding of bindings) {
      const receiver = binding.isStatic ? owner : instance;
      if (typeof receiver?.[binding.method] !== 'function') continue;
      const list = this.monitors.get(binding.step) ?? [];
      list.push({ owner, method: binding.method, receiver, ...(binding.annotation ? { annotation: binding.annotation } : {}) });
      this.monitors.set(binding.step, list);
      this.logger.debug(`Added lifecycle monitor ${owner.name}.${String(binding.method)} for ${binding.step}`);
    }
    return true;
  }

  deregister(owner: Function): void {
    for (const [step, list] of this.monitors) {
      this.monitors.set(
        step,
        list.filter((monitor) => monitor.owner !== owner),
      );
    }
  }

  count(step?: ProcessStep): number {
    if (step) return this.monitors.get(step)?.length ?? 0;
    let total = 0;
    for (const list of this.monitors.values()) total += list.length;
    return total;
  }

  getCalls(): number {
    return this.calls;
  }

  async call(step: ProcessStep, runnable: MessageRunnable): Promise<void> {
    const list = [...(this.monitors.get(step) ?? [])];
    if (list.length === 0) return;
    this.calls++;
    try {
      const annotations = readMeta(runnable.consumer.handlerClass)?.annotations;
      for (const monitor of list) {
        try {
          if (monitor.annotation) {
            if (!annotations?.has(monitor.annotation.key)) continue;
            await monitor.receiver[monitor.method](runnable, annotations.get(monitor.annotation.key));
          } else {
            await monitor.receiver[monitor.method](runnable);
          }
        } catch (error) {
          this.logger.error(`Error invoking lifecycle monitor ${monitor.owner.name}.${String(monitor.method)}: ${describeError(error)}`);
        }
      }
    } finally {
      this.calls--;
    }
  }
}
