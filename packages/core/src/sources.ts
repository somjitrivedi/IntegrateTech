abstract class SourceBase {
  contextClass?: Function;

  setContextClass(contextClass: Function): this {
    this.contextClass = contextClass;
    return this;
  }
}

export class QueueSource extends SourceBase {
  readonly kind = 'queue' as const;

  constructor(readonly name: string) {
    super();
  }

  isLogical(): boolean {
    return false;
  }

  label(): string {
    return this.name;
  }

  equals(other: unknown): boolean {
    return other instanceof QueueSource && other.name === this.name;
  }

  override toString(): string {
    return `QueueSource(name=${this.name})`;
  }
}

export class TopicSource extends SourceBase {
  readonly kind = 'topic' as const;

  constructor(readonly name: string) {
    super();
  }

  isLogical(): boolean {
    return false;
  }

  label(): string {
    return this.name;
  }

  equals(other: unknown): boolean {
    return other instanceof TopicSource && other.name === this.name;
  }

  override toString(): string {
    return `TopicSource(name=${this.name})`;
  }
}

export class GroupedTopicSource extends SourceBase {
  readonly kind = 'groupedTopic' as const;

  constructor(
    readonly name: string,
    readonly group = '',
  ) {
    super();
  }

  isLogical(): boolean {
    return false;
  }

  isGroupDefined(): boolean {
    return this.group !== '';
  }

  label(): string {
    return this.name;
  }

  equals(other: unknown): boolean {
    return other instanceof GroupedTopicSource && other.name === this.name && other.group === this.group;
  }

  override toString(): string {
    return `GroupedTopicSource(name=${this.name}, group=${this.group})`;
  }
}

export class RoleInputSource extends SourceBase {
  readonly kind = 'roleInput' as const;

  constructor(private readonly role = '') {
    super();
  }

  isLogical(): boolean {
    return true;
  }

  isRoleDefined(): boolean {
    return this.getRole() !== undefined;
  }

  getRole(): string | undefined {
    if (this.role !== '') return this.role;
    const fallback = this.contextClass?.name;
    return fallback ? fallback : undefined;
  }

  label(): string {
    return this.getRole() ?? '';
  }

  equals(other: unknown): boolean {
    return other instanceof RoleInputSource && other.getRole() === this.getRole();
  }

  override toString(): string {
    return `RoleInputSource(role=${this.getRole()})`;
  }
}

export type ActionSource = QueueSource | TopicSource | GroupedTopicSource;
export type Source = ActionSource | RoleInputSource;

export interface LogicalSourceContext {
  mapSource(source: RoleInputSource): ActionSource;
}

export interface SourceCases<R> {
  queue: (source: QueueSource) => R;
  topic: (source: TopicSource) => R;
  groupedTopic: (source: GroupedTopicSource) => R;
  roleInput: (source: RoleInputSource) => R;
}

export function matchSource<R>(source: Source, cases: SourceCases<R>): R {
  switch (source.kind) {
    case 'queue':
      return cases.queue(source);
    case 'topic':
      return cases.topic(source);
    case 'groupedTopic':
      return cases.groupedTopic(source);
    case 'roleInput':
      return cases.roleInput(source);
  }
}

export const ROLE_QUEUE_PREFIX = 'integrate-role-';

export function roleQueueName(role: string): string {
  return `${ROLE_QUEUE_PREFIX}${role}`;
}
