#!/usr/bin/env node
// Runtime smoke-matrix. Runs the same test file under every runtime that's
// installed on PATH and reports pass/fail per runtime. PG_URL must be set.

import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';
import { existsSync } from 'node:fs';

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(here, '..');
const testFile = resolve(repoRoot, '__test__/smoke.test.mts');

if (!process.env.PG_URL) {
  console.error('PG_URL is required');
  process.exit(2);
}
if (!existsSync(testFile)) {
  console.error(`test file missing: ${testFile}`);
  process.exit(2);
}

interface Runtime {
  name: string;
  cmd: string;
  args: string[];
}

const runtimes: Runtime[] = [
  { name: 'node', cmd: 'node', args: ['--test', testFile] },
  { name: 'bun', cmd: 'bun', args: ['test', testFile] },
  {
    name: 'deno',
    cmd: 'deno',
    args: [
      'test',
      '--allow-all',
      '--unstable-node-globals',
      '--unstable-detect-cjs',
      '--no-check',
      testFile,
    ],
  },
];

function which(cmd: string): Promise<string | null> {
  return new Promise((res) => {
    const p = spawn('which', [cmd], { stdio: ['ignore', 'pipe', 'ignore'] });
    let out = '';
    p.stdout.on('data', (b: Buffer) => (out += b.toString()));
    p.on('close', (code: number | null) => res(code === 0 ? out.trim() : null));
  });
}

interface RunResult {
  rt: Runtime;
  code: number | null;
  ms: number;
  stdout: string;
  stderr: string;
}

function runOne(rt: Runtime): Promise<RunResult> {
  return new Promise((res) => {
    const start = Date.now();
    const child = spawn(rt.cmd, rt.args, {
      cwd: repoRoot,
      env: process.env,
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let stdout = '';
    let stderr = '';
    child.stdout!.on('data', (b: Buffer) => (stdout += b.toString()));
    child.stderr!.on('data', (b: Buffer) => (stderr += b.toString()));
    child.on('close', (code: number | null) => {
      res({ rt, code, ms: Date.now() - start, stdout, stderr });
    });
  });
}

type Entry = (RunResult & { skipped?: false }) | { rt: Runtime; skipped: true };

const results: Entry[] = [];
for (const rt of runtimes) {
  const found = await which(rt.cmd);
  if (!found) {
    results.push({ rt, skipped: true });
    continue;
  }
  process.stdout.write(`running ${rt.name}... `);
  const r = await runOne(rt);
  results.push(r);
  console.log(r.code === 0 ? `ok (${r.ms}ms)` : `FAIL (${r.ms}ms)`);
}

console.log('\n== matrix summary ==');
let anyFail = false;
for (const r of results) {
  if (r.skipped) {
    console.log(`${r.rt.name.padEnd(6)} skipped (not installed)`);
    continue;
  }
  const status = r.code === 0 ? 'pass' : 'FAIL';
  if (r.code !== 0) anyFail = true;
  console.log(`${r.rt.name.padEnd(6)} ${status}  ${r.ms}ms`);
}
for (const r of results) {
  if (!r.skipped && r.code !== 0) {
    console.error(`\n--- ${r.rt.name} stdout ---\n${r.stdout}`);
    console.error(`--- ${r.rt.name} stderr ---\n${r.stderr}`);
  }
}
process.exit(anyFail ? 1 : 0);
