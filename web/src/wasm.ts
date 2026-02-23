export type CompileOutput = {
  ok: boolean;
  diagnostics: string;
};

export type RunOutput = {
  tables_json: string;
  logs_json: string;
  explain: string;
};

export type WasmApi = {
  compile: (program: string) => CompileOutput;
  run: (program: string, fixtures: string) => RunOutput;
};

function parseJson<T>(text: string, fallback: T): T {
  try {
    return JSON.parse(text) as T;
  } catch {
    return fallback;
  }
}

export async function loadWasmApi(): Promise<WasmApi> {
  const wasmModuleUrl = new URL('/wasm/dsl_wasm.js', window.location.origin).href;

  try {
    const module = await import(/* @vite-ignore */ wasmModuleUrl);
    if (module.default) {
      await module.default();
    }

    return {
      compile: (program: string) =>
        parseJson<CompileOutput>(module.compile(program), {
          ok: false,
          diagnostics: 'failed to parse compile output',
        }),
      run: (program: string, fixtures: string) =>
        parseJson<RunOutput>(module.run(program, fixtures), {
          tables_json: '{}',
          logs_json: '{}',
          explain: 'failed to parse run output',
        }),
    };
  } catch (error) {
    const lastError = error instanceof Error ? error.message : String(error);

    return {
      compile: () => ({
        ok: false,
        diagnostics:
          'WASM package not built. Run `npm run dev` (or `npm run build`) in `web/` so `scripts/prepare-wasm.mjs` can prepare `/public/wasm`.\n' +
          'If wasm-pack is not installed, install it and run `wasm-pack build crates/dsl_wasm --target web --out-dir pkg` from repo root.',
      }),
      run: () => ({
        tables_json: '{}',
        logs_json: '{}',
        explain: `WASM package not built. Could not load ${wasmModuleUrl} (${lastError})`,
      }),
    };
  }
}
