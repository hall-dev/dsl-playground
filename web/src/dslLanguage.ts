import { StreamLanguage, syntaxHighlighting, HighlightStyle } from '@codemirror/language';
import { tags } from '@lezer/highlight';

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

const operators = [':=', '|>', '>>', '~'];

const identifierPattern = /^[A-Za-z_][A-Za-z0-9_.]*/;

export const dslLanguage = StreamLanguage.define({
  token(stream) {
    if (stream.eatSpace()) {
      return null;
    }

    if (stream.match('//')) {
      stream.skipToEnd();
      return 'comment';
    }

    if (stream.match('#')) {
      stream.skipToEnd();
      return 'comment';
    }

    if (stream.peek() === '"') {
      stream.next();
      while (!stream.eol()) {
        const ch = stream.next();
        if (ch === '"') {
          break;
        }
        if (ch === '\\') {
          stream.next();
        }
      }
      return 'string';
    }

    if (stream.match(/^\d+/)) {
      return 'number';
    }

    for (const operator of operators) {
      if (stream.match(operator)) {
        return 'operator';
      }
    }

    const identifier = stream.match(identifierPattern);
    if (identifier) {
      const value = identifier[0];
      if (stageKeywords.has(value)) {
        return 'keyword';
      }
      return 'variableName';
    }

    stream.next();
    return null;
  },
});

const dslHighlightStyle = HighlightStyle.define([
  { tag: tags.keyword, color: '#7c3aed', fontWeight: '600' },
  { tag: tags.operator, color: '#2563eb' },
  { tag: tags.string, color: '#047857' },
  { tag: tags.number, color: '#b45309' },
  { tag: tags.variableName, color: '#111827' },
  { tag: tags.comment, color: '#6b7280', fontStyle: 'italic' },
]);

export const dslSyntaxHighlighting = syntaxHighlighting(dslHighlightStyle);
