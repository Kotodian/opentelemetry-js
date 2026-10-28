#!/usr/bin/env node

// Use npm CLI for an interactive local release. Unlike Lerna's OTP prompt,
// npm CLI opens the registry's security-key challenge in a browser.
import { spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(fileURLToPath(new URL('..', import.meta.url)));
const [workspace, tag = 'latest'] = process.argv.slice(2);

if (!workspace || !/^[a-z0-9][a-z0-9._/-]*$/i.test(workspace) || !/^[a-z0-9][a-z0-9._-]*$/i.test(tag)) {
  console.error('Usage: npm run publish:local -- <workspace-path> [dist-tag]');
  process.exit(1);
}

const directory = resolve(root, workspace);
if (!directory.startsWith(root + sep)) {
  console.error('Workspace must be inside the repository.');
  process.exit(1);
}

let manifest;
try {
  manifest = JSON.parse(readFileSync(resolve(directory, 'package.json'), 'utf8'));
} catch {
  console.error(`No package.json found in ${workspace}.`);
  process.exit(1);
}
if (manifest.private || !manifest.name || !manifest.version) {
  console.error(`Workspace ${workspace} is not a publishable package.`);
  process.exit(1);
}
if (!process.stdin.isTTY || !process.stdout.isTTY) {
  console.error('Local publishing needs an interactive terminal for security-key authentication.');
  process.exit(1);
}

console.log(`Publishing ${manifest.name}@${manifest.version} with dist-tag ${tag}.`);
console.log('Open the npm authentication URL when it appears and use your security key.');
const result = spawnSync('npm', ['publish', '--browser=false', '--tag', tag], {
  cwd: directory,
  env: { ...process.env, NPM_CONFIG_PROVENANCE: 'false' },
  stdio: 'inherit',
});
if (result.error) console.error(result.error.message);
process.exit(result.status ?? 1);
