import { readdir, stat } from 'node:fs/promises';
import { join } from 'node:path';

export interface ScannedFile {
  name: string;
  path: string;
  size: number;
  mtimeMs: number;
}

export interface ScanResult {
  dir: string;
  added: ScannedFile[];
  modified: ScannedFile[];
  removed: ScannedFile[];
}

export interface FileScannerOptions {
  filter?: (name: string) => boolean;
}

export function extensionFilter(...extensions: string[]): (name: string) => boolean {
  const lowered = extensions.map((extension) => extension.toLowerCase());
  return (name) => lowered.some((extension) => name.toLowerCase().endsWith(extension));
}

export function isEmpty(result: ScanResult): boolean {
  return result.added.length === 0 && result.modified.length === 0 && result.removed.length === 0;
}

function sameSignature(a: ScannedFile, b: ScannedFile): boolean {
  return a.size === b.size && a.mtimeMs === b.mtimeMs;
}

export class FileScanner {
  private readonly known = new Map<string, ScannedFile>();
  private readonly pending = new Map<string, ScannedFile>();
  private readonly filter: (name: string) => boolean;

  constructor(
    readonly dir: string,
    options: FileScannerOptions = {},
  ) {
    this.filter = options.filter ?? (() => true);
  }

  files(): ScannedFile[] {
    return [...this.known.values()];
  }

  private async list(): Promise<ScannedFile[]> {
    let names: string[];
    try {
      names = await readdir(this.dir);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') return [];
      throw error;
    }
    const files: ScannedFile[] = [];
    for (const name of names.sort()) {
      if (!this.filter(name)) continue;
      const path = join(this.dir, name);
      try {
        const info = await stat(path);
        if (info.isFile()) files.push({ name, path, size: info.size, mtimeMs: info.mtimeMs });
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
      }
    }
    return files;
  }

  async scan(): Promise<ScanResult> {
    const result: ScanResult = { dir: this.dir, added: [], modified: [], removed: [] };
    const current = await this.list();
    const seen = new Set<string>();
    for (const file of current) {
      seen.add(file.name);
      const known = this.known.get(file.name);
      if (known && sameSignature(known, file)) {
        this.pending.delete(file.name);
        continue;
      }
      const pending = this.pending.get(file.name);
      if (!pending || !sameSignature(pending, file)) {
        this.pending.set(file.name, file);
        continue;
      }
      this.pending.delete(file.name);
      this.known.set(file.name, file);
      (known ? result.modified : result.added).push(file);
    }
    for (const [name, file] of this.known) {
      if (seen.has(name)) continue;
      this.known.delete(name);
      result.removed.push(file);
    }
    for (const name of this.pending.keys()) if (!seen.has(name)) this.pending.delete(name);
    return result;
  }
}
