export enum Header {
  CorrelationId = 'CorrelationId',
  DeliveryCount = 'DeliveryCount',
  DeliveryMode = 'DeliveryMode',
  Expiration = 'Expiration',
  GroupId = 'GroupId',
  GroupSeq = 'GroupSeq',
  PreviousDestination = 'PreviousDestination',
  Priority = 'Priority',
  Producer = 'Producer',
  ReplyTo = 'ReplyTo',
  Result = 'Result',
  ResultDetail = 'ResultDetail',
  RetryCount = 'RetryCount',
  Timestamp = 'Timestamp',
  Type = 'Type',
}

export type HeaderValueType = 'string' | 'number';

export const headerTypes: Readonly<Record<Header, HeaderValueType>> = {
  [Header.CorrelationId]: 'string',
  [Header.DeliveryCount]: 'number',
  [Header.DeliveryMode]: 'number',
  [Header.Expiration]: 'number',
  [Header.GroupId]: 'string',
  [Header.GroupSeq]: 'number',
  [Header.PreviousDestination]: 'string',
  [Header.Priority]: 'number',
  [Header.Producer]: 'string',
  [Header.ReplyTo]: 'string',
  [Header.Result]: 'string',
  [Header.ResultDetail]: 'string',
  [Header.RetryCount]: 'number',
  [Header.Timestamp]: 'number',
  [Header.Type]: 'string',
};

export type HeaderValue = string | number;
export type HeaderMap = Partial<Record<Header, HeaderValue>>;

const headerNames = new Set<string>(Object.values(Header));

export function isHeader(value: string): value is Header {
  return headerNames.has(value);
}

export function sanitizeHeaders(input: Record<string, unknown> | undefined | null): HeaderMap {
  const result: HeaderMap = {};
  if (!input) return result;
  for (const [key, value] of Object.entries(input)) {
    if (!isHeader(key) || value === undefined || value === null) continue;
    if (headerTypes[key] === 'number') {
      const numeric = typeof value === 'number' ? value : Number(value);
      if (Number.isFinite(numeric)) result[key] = numeric;
    } else {
      result[key] = String(value);
    }
  }
  return result;
}
