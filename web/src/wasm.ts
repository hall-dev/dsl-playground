export type WasmApi = {
  compile: (program: string) => string;
  run: (program: string, fixtures: string) => string;
};

export async function loadWasmApi(): Promise<WasmApi> {
  try {
    const module = await import('../../crates/dsl_wasm/pkg/dsl_wasm.js');
    if (module.default) {
      await module.default();
    }
    return { compile: module.compile, run: module.run };
  } catch {
    return {
      compile: () => JSON.stringify({ ok: false, diagnostics: 'WASM package not built yet' }),
      run: () => JSON.stringify({ tables_json: '{}', logs_json: '{}', explain: 'WASM package not built yet' }),
    };
  }
}
