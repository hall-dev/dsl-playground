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
  try {
    const wasmModulePath = '../../crates/dsl_wasm/pkg/dsl_wasm.js';
    const module = await import(/* @vite-ignore */ wasmModulePath);
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
  } catch {
    return {
      compile: () => ({ ok: false, diagnostics: 'WASM package not built yet' }),
      run: () => ({ tables_json: '{}', logs_json: '{}', explain: 'WASM package not built yet' }),
    };
  }
}
