import { readMeta } from './metadata';

export class ConsumerService {
  readonly instance: any;
  readonly serviceClass: Function;

  constructor(target: (new () => object) | object) {
    if (typeof target === 'function') {
      this.serviceClass = target;
      this.instance = new (target as new () => object)();
    } else {
      this.serviceClass = (target as { constructor: Function }).constructor;
      this.instance = target;
    }
  }

  private methods(select: 'beanResolvers' | 'onDeploy' | 'onUndeploy') {
    return readMeta(this.serviceClass)?.[select] ?? [];
  }

  async resolve(type: unknown, name: string | undefined): Promise<unknown> {
    for (const method of this.methods('beanResolvers')) {
      const value = await this.instance[method](type, name);
      if (value !== undefined && value !== null) return value;
    }
    return undefined;
  }

  async onDeploy(): Promise<void> {
    for (const method of this.methods('onDeploy')) await this.instance[method]();
  }

  async onUndeploy(): Promise<void> {
    for (const method of this.methods('onUndeploy')) await this.instance[method]();
  }
}
