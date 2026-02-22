import { useEffect, useMemo, useState } from 'react';
import { loadWasmApi, type RunOutput, type WasmApi } from './wasm';

type Example = {
  name: string;
  program: string;
  fixtures: string;
};

const examples: Example[] = [
  {
    name: 'A. Map/filter',
    program: `xs := input.json("xs") |> json;
xs |> map(_ + 1) |> filter(_ > 2) |> ui.table("out");`,
    fixtures: '{"xs":[1,2,3]}',
  },
  {
    name: 'B. Roundtrip',
    program: `chain := base64 >> ~base64;
input.json("bs") |> chain |> ui.table("t");`,
    fixtures: '{"bs":["AQID","SGVsbG8="]}',
  },
  {
    name: 'C. UTF8',
    program: `input.json("ss") |> json |> utf8 |> ~utf8 |> ui.table("rt");`,
    fixtures: '{"ss":["hello","world"]}',
  },
];

const pretty = (value: string) => {
  try {
    return JSON.stringify(JSON.parse(value), null, 2);
  } catch {
    return value;
  }
};

export function App() {
  const [api, setApi] = useState<WasmApi | null>(null);
  const [program, setProgram] = useState(examples[0].program);
  const [fixtures, setFixtures] = useState(examples[0].fixtures);
  const [runOutput, setRunOutput] = useState<RunOutput>({
    explain: 'Load WASM and click Run.',
    tables_json: '{}',
    logs_json: '{}',
  });
  const [status, setStatus] = useState('Loading WASM module...');

  useEffect(() => {
    loadWasmApi().then((loaded) => {
      setApi(loaded);
      setStatus('WASM loaded.');
    });
  }, []);

  const prettyTables = useMemo(() => pretty(runOutput.tables_json), [runOutput.tables_json]);
  const prettyLogs = useMemo(() => pretty(runOutput.logs_json), [runOutput.logs_json]);

  return (
    <main style={{ fontFamily: 'sans-serif', padding: 16, maxWidth: 1000, margin: '0 auto' }}>
      <h1>DSL Playground (v0)</h1>
      <p>{status}</p>

      <label>
        Example program:{' '}
        <select
          onChange={(e) => {
            const example = examples.find((x) => x.name === e.target.value);
            if (!example) {
              return;
            }
            setProgram(example.program);
            setFixtures(example.fixtures);
          }}
          defaultValue={examples[0].name}
        >
          {examples.map((example) => (
            <option key={example.name} value={example.name}>
              {example.name}
            </option>
          ))}
        </select>
      </label>

      <h2>Program</h2>
      <textarea
        value={program}
        onChange={(e) => setProgram(e.target.value)}
        rows={8}
        style={{ width: '100%', fontFamily: 'monospace' }}
      />

      <h2>Fixtures JSON</h2>
      <textarea
        value={fixtures}
        onChange={(e) => setFixtures(e.target.value)}
        rows={6}
        style={{ width: '100%', fontFamily: 'monospace' }}
      />

      <div style={{ marginTop: 10 }}>
        <button
          onClick={() => {
            if (!api) {
              setStatus('WASM still loading...');
              return;
            }
            setRunOutput(api.run(program, fixtures));
          }}
        >
          Run
        </button>
      </div>

      <h2>Explain</h2>
      <pre style={{ background: '#f5f5f5', padding: 12 }}>{runOutput.explain}</pre>

      <h2>Tables JSON</h2>
      <pre style={{ background: '#f5f5f5', padding: 12 }}>{prettyTables}</pre>

      <h2>Logs JSON</h2>
      <pre style={{ background: '#f5f5f5', padding: 12 }}>{prettyLogs}</pre>
    </main>
  );
}
