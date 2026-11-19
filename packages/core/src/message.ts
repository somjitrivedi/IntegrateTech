import { type HeaderMap, sanitizeHeaders } from './headers';

export class IntegrateMessage {
  id?: string;
  body?: string;
  flagged = false;
  warning?: string;
  properties: Record<string, string> = {};
  headers: HeaderMap = {};

  constructor(body?: string) {
    if (body !== undefined) this.body = body;
  }

  async acknowledge(): Promise<boolean> {
    return true;
  }

  async beforeProcessing(): Promise<boolean> {
    return true;
  }

  setProperties(properties: Record<string, string>): void {
    this.properties = { ...properties };
  }

  setHeaders(headers: HeaderMap): void {
    this.headers = sanitizeHeaders(headers);
  }
}
