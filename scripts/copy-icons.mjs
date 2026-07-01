// Copy node and credential icons next to their compiled .js, since tsc only
// emits TypeScript output. n8n resolves `icon: 'file:qod.svg'` relative to
// the file that references it (node or credential).
import { cpSync, mkdirSync } from 'node:fs';

mkdirSync('dist/nodes/QuackOnDemand', { recursive: true });
cpSync('nodes/QuackOnDemand/qod.svg', 'dist/nodes/QuackOnDemand/qod.svg');

mkdirSync('dist/credentials', { recursive: true });
cpSync('nodes/QuackOnDemand/qod.svg', 'dist/credentials/qod.svg');

console.log('copied node icons to dist');
