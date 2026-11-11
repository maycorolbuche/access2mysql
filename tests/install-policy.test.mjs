import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

function readManifest(name) {
  return JSON.parse(readFileSync(new URL('../' + name, import.meta.url), 'utf8'));
}

test('os manifestos não declaram ganchos automáticos de instalação', () => {
  const manifest = readManifest('package.json');
  const lock = readManifest('package-lock.json');
  const hooks = ['preinstall', 'install', 'postinstall', 'prepublish', 'preprepare', 'prepare', 'postprepare'];
  for (const [name, scripts] of [
    ['package.json', manifest.scripts ?? {}],
    ['package-lock.json (raiz)', lock.packages['']?.scripts ?? {}],
  ]) {
    for (const hook of hooks) {
      assert.equal(Object.hasOwn(scripts, hook), false, name + ' contém o gancho ' + hook);
    }
  }
  for (const [path, pkg] of Object.entries(lock.packages)) {
    assert.notEqual(pkg.hasInstallScript, true, path + ' declara script de instalação');
  }
});

test('dependências do lockfile usam tarballs do registro npm com integridade', () => {
  const { packages } = readManifest('package-lock.json');
  const entries = Object.entries(packages).filter(([path]) => path !== '');
  assert.ok(entries.length > 0);
  for (const [path, pkg] of entries) {
    const url = new URL(pkg.resolved);
    assert.equal(url.origin, 'https://registry.npmjs.org', path);
    assert.equal(url.username + url.password, '', path);
    assert.match(url.pathname, /\.tgz$/, path);
    assert.match(pkg.integrity ?? '', /^sha(?:1|256|384|512)-[A-Za-z0-9+/]+={0,2}$/, path);
  }
});
