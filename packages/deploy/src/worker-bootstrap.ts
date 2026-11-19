import { existsSync, readFileSync } from 'node:fs';
import { registerHooks } from 'node:module';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { workerData } from 'node:worker_threads';
import ts from 'typescript';

const compilerOptions: ts.CompilerOptions = {
  module: ts.ModuleKind.ESNext,
  target: ts.ScriptTarget.ES2022,
  experimentalDecorators: true,
  useDefineForClassFields: false,
  sourceMap: false,
  inlineSourceMap: true,
  inlineSources: true,
};

function isTypeScript(url: string): boolean {
  return url.startsWith('file:') && url.endsWith('.ts') && !url.endsWith('.d.ts') && !url.includes('/node_modules/');
}

function candidates(specifier: string, parentURL: string | undefined): string[] {
  if (!parentURL?.startsWith('file:')) return [];
  if (!specifier.startsWith('./') && !specifier.startsWith('../') && !specifier.startsWith('/') && !specifier.startsWith('file:')) return [];
  const base = new URL(specifier, parentURL);
  return [`${base.href}.ts`, new URL('index.ts', `${base.href}/`).href];
}

registerHooks({
  resolve(specifier, context, nextResolve) {
    try {
      return nextResolve(specifier, context);
    } catch (error) {
      for (const candidate of candidates(specifier, context.parentURL)) {
        if (existsSync(fileURLToPath(candidate))) return { url: candidate, format: 'module', shortCircuit: true };
      }
      throw error;
    }
  },
  load(url, context, nextLoad) {
    if (!isTypeScript(url)) return nextLoad(url, context);
    const fileName = fileURLToPath(url);
    const output = ts.transpileModule(readFileSync(fileName, 'utf8'), { compilerOptions, fileName });
    return { format: 'module', source: output.outputText, shortCircuit: true };
  },
});

const data = workerData as { entry?: string } | null;
const entry = data?.entry ? pathToFileURL(data.entry).href : new URL('./worker-host.ts', import.meta.url).href;
await import(entry);
