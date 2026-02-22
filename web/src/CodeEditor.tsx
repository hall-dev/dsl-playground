import { useMemo } from 'react';

type CodeEditorProps = {
  value: string;
  onChange: (value: string) => void;
};

const stageKeywords = new Set([
  'input.json',
  'map',
  'filter',
  'flat_map',
  'json',
  'utf8',
  'base64',
  'kv.load',
  'lookup.kv',
  'lookup.batch_kv',
  'group.collect_all',
  'group.topn_items',
  'rank.topk',
  'ui.table',
  'ui.log',
  'rbac.evaluate',
]);

function escapeHtml(value: string): string {
  return value
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;');
}

function highlight(value: string): string {
  const tokenPattern = /(:=|\|>|>>|~)|("(?:\\.|[^"])*")|(\b\d+\b)|([A-Za-z_][A-Za-z0-9_.]*)/g;
  let out = '';
  let index = 0;

  for (const match of value.matchAll(tokenPattern)) {
    const start = match.index ?? 0;
    out += escapeHtml(value.slice(index, start));

    const [full, operator, stringLiteral, numberLiteral, identifier] = match;

    if (operator) {
      out += `<span class="tok-operator">${escapeHtml(full)}</span>`;
    } else if (stringLiteral) {
      out += `<span class="tok-string">${escapeHtml(full)}</span>`;
    } else if (numberLiteral) {
      out += `<span class="tok-number">${escapeHtml(full)}</span>`;
    } else if (identifier && stageKeywords.has(identifier)) {
      out += `<span class="tok-keyword">${escapeHtml(full)}</span>`;
    } else {
      out += escapeHtml(full);
    }

    index = start + full.length;
  }

  out += escapeHtml(value.slice(index));
  return out;
}

export function CodeEditor({ value, onChange }: CodeEditorProps) {
  const highlighted = useMemo(() => highlight(value), [value]);

  return (
    <div className="code-editor">
      <pre className="code-editor__highlight" aria-hidden="true" dangerouslySetInnerHTML={{ __html: highlighted + '\n' }} />
      <textarea
        className="code-editor__input"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        spellCheck={false}
      />
    </div>
  );
}
