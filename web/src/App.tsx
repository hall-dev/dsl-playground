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

type TableValue = Record<string, unknown[]>;

function parseTablesJson(tablesJson: string): TableValue {
  try {
    const parsed = JSON.parse(tablesJson) as unknown;
    if (parsed && typeof parsed === 'object') {
      return Object.entries(parsed as Record<string, unknown>).reduce<TableValue>((acc, [name, rows]) => {
        acc[name] = Array.isArray(rows) ? rows : [];
        return acc;
      }, {});
    }
  } catch {
    // Keep fallback below.
  }
  return {};
}

function renderTableRows(name: string, rows: unknown[]) {
  const allRecords = rows.every((row) => row && typeof row === 'object' && !Array.isArray(row));

  if (!allRecords) {
    return (
      <table key={name} border={1} cellPadding={6} style={{ borderCollapse: 'collapse', marginBottom: 12 }}>
        <thead>
          <tr>
            <th>value</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row, index) => (
            <tr key={index}>
              <td>
                <code>{JSON.stringify(row)}</code>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    );
  }

  const columns = Array.from(
    new Set(rows.flatMap((row) => Object.keys(row as Record<string, unknown>))),
  ).sort();

  return (
    <table key={name} border={1} cellPadding={6} style={{ borderCollapse: 'collapse', marginBottom: 12 }}>
      <thead>
        <tr>
          {columns.map((column) => (
            <th key={column}>{column}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.map((row, index) => {
          const record = row as Record<string, unknown>;
          return (
            <tr key={index}>
              {columns.map((column) => (
                <td key={column}>
                  <code>{JSON.stringify(record[column] ?? null)}</code>
                </td>
              ))}
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

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
  const parsedTables = useMemo(() => parseTablesJson(runOutput.tables_json), [runOutput.tables_json]);

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

      <h2>Tables</h2>
      {Object.keys(parsedTables).length === 0 ? (
        <pre style={{ background: '#f5f5f5', padding: 12 }}>{prettyTables}</pre>
      ) : (
        Object.entries(parsedTables).map(([name, rows]) => (
          <section key={name} style={{ marginBottom: 12 }}>
            <h3 style={{ marginBottom: 8 }}>{name}</h3>
            {renderTableRows(name, rows)}
          </section>
        ))
      )}

      <h2>Logs JSON</h2>
      <pre style={{ background: '#f5f5f5', padding: 12 }}>{prettyLogs}</pre>
    </main>
  );
}
