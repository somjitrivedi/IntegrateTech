import { MemoryConnector } from '@integratetech/connector-memory';
import {
  BeanResolver,
  Config,
  CoreService,
  Inject,
  IntegrateTech,
  Message,
  OnDeploy,
  OnMessage,
  OnUndeploy,
  Queue,
  Service,
} from '@integratetech/core';
import { describe, expect, it } from 'vitest';
import { ClassDeployment, DeploymentError, DeploymentManager, DeploymentRunner, inspectClasses, inspectModule } from '../src/index';
import { recordingLogger, waitFor } from './helpers';

const log: string[] = [];

class Greeter {
  greet(name: string) {
    return `hi ${name}`;
  }
}

@Service()
class GreeterService {
  @OnDeploy()
  deployed() {
    log.push('deploy');
  }

  @OnUndeploy()
  undeployed() {
    log.push('undeploy');
  }

  @BeanResolver()
  resolve(type: unknown) {
    return type === Greeter ? new Greeter() : undefined;
  }
}

@CoreService()
class Audit {}

@Queue('greetings')
class GreetingConsumer {
  @Message() body!: string;
  @Inject(Greeter) greeter!: Greeter;
  @Config('app', 'suffix') suffix!: string;

  @OnMessage()
  async handle() {
    const [name, delay] = this.body.split(':');
    log.push(`start:${name}`);
    await new Promise((resolve) => setTimeout(resolve, Number(delay ?? 0)));
    log.push(`${this.greeter.greet(name!)}${this.suffix ?? ''}`);
  }
}

class NotAConsumer {}

async function started() {
  const logger = recordingLogger();
  const integrate = new IntegrateTech({ logger });
  const connector = new MemoryConnector({ pollIntervalMs: 2 });
  await integrate.start(connector);
  return { integrate, connector, logger };
}

describe('inspecting deployment contents', () => {
  it('separates consumers from services and ignores everything else', () => {
    const contents = inspectModule({ GreetingConsumer, GreeterService, Audit, NotAConsumer, value: 3, default: GreetingConsumer });
    expect(contents.consumers).toEqual([GreetingConsumer]);
    expect(contents.services).toEqual([GreeterService, Audit]);
    expect(inspectClasses([NotAConsumer, null, 'x']).consumers).toEqual([]);
  });
});

describe('ClassDeployment', () => {
  it('runs services, injects beans and config, then drains in-flight work on stop', async () => {
    log.length = 0;
    const { integrate, connector } = await started();
    integrate.properties.set('app', { suffix: '!' });
    const deployment = new ClassDeployment('greeter', [GreetingConsumer, GreeterService], integrate, { receiveTimeoutMs: 20 });
    expect(deployment.info()).toMatchObject({ name: 'greeter', kind: 'class', status: 'idle', consumers: ['GreetingConsumer'], services: ['GreeterService'], configFiles: ['app'] });
    await deployment.start();
    expect(deployment.info().status).toBe('running');
    expect(log).toEqual(['deploy']);
    await connector.getQueueProducer('greetings').send('ann:0');
    await waitFor(() => log.includes('hi ann!'));
    await connector.getQueueProducer('greetings').send('bob:150');
    await waitFor(() => log.includes('start:bob'));
    expect(deployment.processed()).toBe(1);
    expect(await deployment.stop()).toEqual({ drained: true });
    expect(log).toEqual(['deploy', 'start:ann', 'hi ann!', 'start:bob', 'hi bob!', 'undeploy']);
    expect(await connector.depth('greetings')).toBe(0);
    expect(deployment.info()).toMatchObject({ status: 'stopped', generation: 1 });
    await integrate.shutdown();
  });

  it('reports a drain that runs past its timeout', async () => {
    log.length = 0;
    const { integrate, connector } = await started();
    const deployment = new ClassDeployment('slow', [GreetingConsumer, GreeterService], integrate, { receiveTimeoutMs: 20, drainTimeoutMs: 30 });
    await deployment.start();
    await connector.getQueueProducer('greetings').send('slow:400');
    await waitFor(() => log.includes('start:slow'));
    expect(await deployment.stop()).toEqual({ drained: false });
    await waitFor(() => log.includes('hi slow'));
    await integrate.shutdown();
  });

  it('fails to start without consumers', async () => {
    const { integrate } = await started();
    const deployment = new ClassDeployment('empty', [GreeterService], integrate);
    await expect(deployment.start()).rejects.toBeInstanceOf(DeploymentError);
    expect(deployment.info()).toMatchObject({ status: 'failed', error: 'Deployment contains no consumer classes' });
    expect(await deployment.stop()).toEqual({ drained: true });
    expect(deployment.info().status).toBe('failed');
    await integrate.shutdown();
  });

  it('cleans up when a service fails during deploy', async () => {
    @Service()
    class Broken {
      @OnDeploy()
      deploy() {
        throw new Error('no database');
      }
    }
    const { integrate, connector } = await started();
    const runner = new DeploymentRunner(integrate, inspectClasses([GreetingConsumer, Broken]));
    await expect(runner.start()).rejects.toThrow('no database');
    expect(runner.isStarted()).toBe(false);
    expect(connector.openConsumers()).toBe(0);
    await integrate.shutdown();
  });
});

describe('DeploymentManager with class deployments', () => {
  it('registers, replaces and unregisters in-process deployments', async () => {
    log.length = 0;
    const { integrate, connector } = await started();
    const manager = new DeploymentManager({ integrate, runner: { receiveTimeoutMs: 20 } });
    const events: string[] = [];
    manager.on((event) => events.push(`${event.type}:${'name' in event ? event.name : ''}`));
    await manager.start();
    const info = await manager.registerDeployment('greeter', GreetingConsumer, GreeterService);
    expect(info).toMatchObject({ status: 'running', kind: 'class' });
    await connector.getQueueProducer('greetings').send('cy:0');
    await waitFor(() => log.includes('hi cy'));
    await manager.registerDeployment('greeter', GreetingConsumer, GreeterService);
    expect(manager.list().map((entry) => entry.name)).toEqual(['greeter']);
    await manager.unregisterDeployment('greeter');
    expect(manager.list()).toEqual([]);
    expect(events).toEqual(['deployed:greeter', 'redeployed:greeter', 'undeployed:greeter']);
    expect(log).toEqual(['deploy', 'start:cy', 'hi cy', 'undeploy', 'deploy', 'undeploy']);
    await manager.stop();
    await integrate.shutdown();
  });

  it('rejects class deployments without an IntegrateTech and file deploys without a connector', async () => {
    await expect(new DeploymentManager().registerDeployment('x', GreetingConsumer)).rejects.toThrow('Class deployments need an IntegrateTech instance');
    expect(() => new DeploymentManager({ deployDir: 'deploy' })).toThrow('A connector spec is required');
  });
});
