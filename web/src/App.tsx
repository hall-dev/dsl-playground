import { useEffect, useState } from 'react';
import { loadWasmApi, type WasmApi } from './wasm';

const defaultProgram = `xs := input.json("xs") |> ~json;
xs |> map(_ + 1) |> filter(_ > 2) |> ui.table("out");`;

const defaultFixtures = `{"xs":[1,2,3]}`;

export function App() {
  const [api, setApi] = useState<WasmApi | null>(null);
  const [program, setProgram] = useState(defaultProgram);
  const [fixtures, setFixtures] = useState(defaultFixtures);
  const [output, setOutput] = useState('Load WASM and click run.');

  useEffect(() => {
    loadWasmApi().then(setApi);
  }, []);

  return (
    <main style={{ fontFamily: 'sans-serif', padding: 16, maxWidth: 900, margin: '0 auto' }}>
      <h1>DSL Playground (v0)</h1>
      <p>Minimal UI: program + fixtures JSON + run output.</p>
      <textarea value={program} onChange={(e) => setProgram(e.target.value)} rows={8} style={{ width: '100%' }} />
      <textarea value={fixtures} onChange={(e) => setFixtures(e.target.value)} rows={4} style={{ width: '100%', marginTop: 8 }} />
      <div style={{ marginTop: 8 }}>
        <button
          onClick={() => {
            if (!api) return;
            setOutput(api.run(program, fixtures));
          }}
        >
          Run
        </button>
      </div>
      <pre style={{ background: '#f5f5f5', padding: 12, marginTop: 12 }}>{output}</pre>
    </main>
  );
}
