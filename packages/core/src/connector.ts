import type { IntegrateMessage } from './message';
import type { MessageProducer } from './producer';
import type { LogicalSourceContext, Source } from './sources';

export interface MessageConsumer {
  receive(timeoutMs: number, signal?: AbortSignal): Promise<IntegrateMessage | null>;
  close(): Promise<void>;
}

export interface Connector extends LogicalSourceContext {
  start(): Promise<void>;
  stop(): Promise<void>;
  getConsumerForSource(source: Source): Promise<MessageConsumer>;
  getQueue(name: string): Promise<MessageConsumer>;
  getTopic(name: string): Promise<MessageConsumer>;
  getQueueProducer(name: string): MessageProducer;
  getTopicProducer(name: string): MessageProducer;
  supportsOrder(): boolean;
  supportsResponse(): boolean;
}
