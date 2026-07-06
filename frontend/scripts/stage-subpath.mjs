/**
 * Copies the production browser build into demo/financial-reporting/ so deployed
 * URLs match the Angular baseHref (/demo/financial-reporting/).
 */
import { cpSync, mkdirSync, readdirSync, rmSync } from 'fs';
import { join } from 'path';

const SUBPATH = 'demo/financial-reporting';
const browserRoot = join(process.cwd(), 'dist/frontend/browser');
const targetRoot = join(browserRoot, SUBPATH);

mkdirSync(targetRoot, { recursive: true });

for (const name of readdirSync(browserRoot)) {
  if (name === 'demo') continue;
  cpSync(join(browserRoot, name), join(targetRoot, name), { recursive: true });
}

for (const name of readdirSync(browserRoot)) {
  if (name === 'demo') continue;
  rmSync(join(browserRoot, name), { recursive: true, force: true });
}

console.log(`Staged build at /${SUBPATH}/`);
