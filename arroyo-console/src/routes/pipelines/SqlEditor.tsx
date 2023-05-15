import Editor from '@monaco-editor/react';
import { Dispatch } from 'react';

export function CodeEditor({
  query,
  setQuery,
  readOnly,
  language,
}: {
  query: string;
  setQuery?: Dispatch<string>;
  readOnly?: boolean;
  language?: string;
}) {
  const onChange = (value: string | undefined) => {
    if (setQuery != null) {
      setQuery(value || '');
    }
  };

  return (
    <Editor
      height="50vh"
      defaultLanguage={language || 'sql'}
      onChange={onChange}
      theme="vs-dark"
      options={{ minimap: { enabled: false }, wordWrap: 'on', readOnly: readOnly || false }}
      value={query}
    />
  );
}
