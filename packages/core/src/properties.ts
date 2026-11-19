const WHITESPACE = new Set([' ', '\t', '\f']);

function stripLeading(line: string): string {
  let index = 0;
  while (index < line.length && WHITESPACE.has(line[index]!)) index++;
  return line.slice(index);
}

function trailingBackslashes(line: string): number {
  let count = 0;
  for (let i = line.length - 1; i >= 0 && line[i] === '\\'; i--) count++;
  return count;
}

function logicalLines(text: string): string[] {
  const lines: string[] = [];
  let current: string | null = null;
  for (const raw of text.split(/\r\n|\r|\n/)) {
    const line = stripLeading(raw);
    if (current === null && (line === '' || line.startsWith('#') || line.startsWith('!'))) continue;
    const continues = trailingBackslashes(line) % 2 === 1;
    current = (current ?? '') + (continues ? line.slice(0, -1) : line);
    if (!continues) {
      lines.push(current);
      current = null;
    }
  }
  if (current !== null) lines.push(current);
  return lines;
}

function unescape(value: string): string {
  let result = '';
  for (let i = 0; i < value.length; i++) {
    const char = value[i]!;
    if (char !== '\\' || i === value.length - 1) {
      result += char;
      continue;
    }
    const next = value[++i]!;
    const hex = value.slice(i + 1, i + 5);
    if (next === 't') result += '\t';
    else if (next === 'n') result += '\n';
    else if (next === 'r') result += '\r';
    else if (next === 'f') result += '\f';
    else if (next === 'u' && /^[0-9a-fA-F]{4}$/.test(hex)) {
      result += String.fromCharCode(parseInt(hex, 16));
      i += 4;
    } else result += next;
  }
  return result;
}

function keyEnd(line: string): number {
  let escaped = false;
  for (let index = 0; index < line.length; index++) {
    const char = line[index]!;
    if (escaped) {
      escaped = false;
      continue;
    }
    if (char === '\\') {
      escaped = true;
      continue;
    }
    if (char === '=' || char === ':' || WHITESPACE.has(char)) return index;
  }
  return line.length;
}

export function parseProperties(text: string): Record<string, string> {
  const result: Record<string, string> = {};
  for (const line of logicalLines(text)) {
    const end = keyEnd(line);
    let start = end;
    while (start < line.length && WHITESPACE.has(line[start]!)) start++;
    if (start < line.length && (line[start] === '=' || line[start] === ':')) start++;
    while (start < line.length && WHITESPACE.has(line[start]!)) start++;
    result[unescape(line.slice(0, end))] = unescape(line.slice(start));
  }
  return result;
}

export class PropertiesRegistry {
  private readonly entries = new Map<string, Record<string, string>>();

  set(name: string, values: Record<string, string>): void {
    this.entries.set(name, { ...values });
  }

  load(name: string, text: string): void {
    this.set(name, parseProperties(text));
  }

  get(name: string): Record<string, string> | undefined {
    return this.entries.get(name);
  }

  delete(name: string): boolean {
    return this.entries.delete(name);
  }

  names(): string[] {
    return [...this.entries.keys()];
  }
}
