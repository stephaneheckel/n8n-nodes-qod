// Post-build: copy package.json to dist/ and strip the "dist/" prefix
// from n8n node/credential paths.  This is required so that
//   N8N_CUSTOM_EXTENSIONS=dist n8n start
// works — n8n reads dist/package.json and resolves paths relative to dist/,
// so "nodes/Foo.js" -> "dist/nodes/Foo.js" (correct) while
//    "dist/nodes/Foo.js" -> "dist/dist/nodes/Foo.js" (broken).
//
// We only patch the dist/ copy; the root package.json keeps the "dist/"
// prefix so the published package works for regular n8n installations.

const fs = require('fs');
const path = require('path');

const rootPkg = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'package.json'), 'utf-8'));

// Strip "dist/" from n8n paths in the dist/ copy
if (rootPkg.n8n) {
  if (rootPkg.n8n.nodes) {
    rootPkg.n8n.nodes = rootPkg.n8n.nodes.map((p) => p.replace(/^dist[\\/]/, ''));
  }
  if (rootPkg.n8n.credentials) {
    rootPkg.n8n.credentials = rootPkg.n8n.credentials.map((p) => p.replace(/^dist[\\/]/, ''));
  }
}

// Also remove "devDependencies" and "scripts" to keep dist/package.json lean
delete rootPkg.devDependencies;
delete rootPkg.scripts;

const distDir = path.join(__dirname, '..', 'dist');
if (!fs.existsSync(distDir)) fs.mkdirSync(distDir, { recursive: true });
fs.writeFileSync(path.join(distDir, 'package.json'), JSON.stringify(rootPkg, null, 2) + '\n');

console.log('dist/package.json patched for N8N_CUSTOM_EXTENSIONS=dist');
