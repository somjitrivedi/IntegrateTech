export function parseKeyValue(input: string | null | undefined): Record<string, string> {
  const result: Record<string, string> = {};
  if (input === null || input === undefined) return result;
  for (const line of input.split('\n')) {
    const separator = line.indexOf('=');
    const key = (separator === -1 ? line : line.slice(0, separator)).trim();
    if (key === '') continue;
    result[key] = separator === -1 ? '' : line.slice(separator + 1);
  }
  return result;
}

export function formatKeyValue(input: Record<string, string> | Map<string, string>): string {
  const entries = input instanceof Map ? [...input.entries()] : Object.entries(input);
  return entries.map(([key, value]) => `${key}=${value}\n`).join('');
}
