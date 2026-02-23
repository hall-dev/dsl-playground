import { existsSync, cpSync, mkdirSync, readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const webDir = resolve(scriptDir, '..');
const repoDir = resolve(webDir, '..');
const wasmCrateTomlPath = resolve(repoDir, 'crates/dsl_wasm/Cargo.toml');
const pkgDir = resolve(repoDir, 'crates/dsl_wasm/pkg');
const outDir = resolve(webDir, 'public/wasm');

function hasBuiltPkg() {
  return existsSync(resolve(pkgDir, 'dsl_wasm.js')) && existsSync(resolve(pkgDir, 'dsl_wasm_bg.wasm'));
}

function hasWasmBindgenDependency() {
  if (!existsSync(wasmCrateTomlPath)) {
    return false;
  }

  const crateToml = readFileSync(wasmCrateTomlPath, 'utf8');
  return /^wasm-bindgen\s*=\s*/m.test(crateToml);
}

if (!hasBuiltPkg()) {
  const wasmPackCheck = spawnSync('wasm-pack', ['--version'], { stdio: 'ignore' });
  if (wasmPackCheck.status === 0) {
    if (hasWasmBindgenDependency()) {
      const build = spawnSync(
        'wasm-pack',
        ['build', 'crates/dsl_wasm', '--target', 'web', '--out-dir', 'pkg'],
        { cwd: repoDir, stdio: 'inherit' },
      );
      if (build.status !== 0) {
        console.warn('[prepare-wasm] wasm-pack build failed; playground will use fallback messaging.');
      }
    } else {
      console.warn(
        '[prepare-wasm] crates/dsl_wasm/Cargo.toml has no wasm-bindgen dependency; skipping wasm-pack build.',
      );
    }
  } else {
    console.warn('[prepare-wasm] wasm-pack is not installed and crates/dsl_wasm/pkg is missing.');
  }
}

if (hasBuiltPkg()) {
  mkdirSync(outDir, { recursive: true });
  cpSync(pkgDir, outDir, { recursive: true });
  console.log('[prepare-wasm] copied WASM package to web/public/wasm.');
}
