import Editor, { Monaco, OnMount } from '@monaco-editor/react';
import React, { Dispatch, useCallback, useEffect, useRef } from 'react';
import { Flex } from '@chakra-ui/react';
import { editor } from 'monaco-editor';
import { SqlDiagnostic } from '../../lib/data_fetching';

const SQL_DIAGNOSTIC_OWNER = 'arroyo-sql-diagnostics';

function diagnosticRange(model: editor.ITextModel, diagnostic: SqlDiagnostic) {
  if (!diagnostic.span) {
    return undefined;
  }

  return model.validateRange({
    startLineNumber: diagnostic.span.start.line,
    startColumn: diagnostic.span.start.column,
    endLineNumber: diagnostic.span.end.line,
    endColumn: diagnostic.span.end.column,
  });
}

export function CodeEditor({
  code,
  setCode,
  readOnly,
  language,
  diagnostics = [],
  selectedDiagnostic,
}: {
  code: string;
  setCode?: Dispatch<string>;
  readOnly?: boolean;
  language?: string;
  diagnostics?: SqlDiagnostic[];
  selectedDiagnostic?: SqlDiagnostic;
}) {
  const editorRef = useRef<editor.IStandaloneCodeEditor>();
  const monacoRef = useRef<Monaco>();

  const onChange = (value: string | undefined) => {
    if (setCode != null) {
      setCode(value || '');
    }
  };

  const updateMarkers = useCallback(
    (mountedEditor: editor.IStandaloneCodeEditor, monaco: Monaco) => {
      const model = mountedEditor.getModel();
      if (!model) {
        return;
      }

      const markers = diagnostics.flatMap(diagnostic => {
        const range = diagnosticRange(model, diagnostic);
        if (!range) {
          return [];
        }

        return [
          {
            ...range,
            message: diagnostic.message,
            severity: monaco.MarkerSeverity.Error,
            source: 'Arroyo SQL',
          },
        ];
      });

      monaco.editor.setModelMarkers(model, SQL_DIAGNOSTIC_OWNER, markers);
    },
    [diagnostics]
  );

  const onMount: OnMount = (mountedEditor, monaco) => {
    editorRef.current = mountedEditor;
    monacoRef.current = monaco;
    updateMarkers(mountedEditor, monaco);
  };

  useEffect(() => {
    if (editorRef.current && monacoRef.current) {
      updateMarkers(editorRef.current, monacoRef.current);
    }
  }, [updateMarkers]);

  useEffect(() => {
    const mountedEditor = editorRef.current;
    const monaco = monacoRef.current;
    const model = mountedEditor?.getModel();
    if (!mountedEditor || !monaco || !model || !selectedDiagnostic) {
      return;
    }

    const range = diagnosticRange(model, selectedDiagnostic);
    if (range) {
      mountedEditor.setSelection(range);
      mountedEditor.revealRangeInCenter(range, monaco.editor.ScrollType.Smooth);
      mountedEditor.focus();
    }
  }, [selectedDiagnostic]);

  useEffect(() => {
    return () => {
      const model = editorRef.current?.getModel();
      if (model && monacoRef.current) {
        monacoRef.current.editor.setModelMarkers(model, SQL_DIAGNOSTIC_OWNER, []);
      }
    };
  }, []);

  return (
    <Flex py={5} pr={5} flex={1}>
      <Editor
        defaultLanguage={language || 'sql'}
        onChange={onChange}
        onMount={onMount}
        theme="vs-dark"
        options={{ minimap: { enabled: false }, wordWrap: 'on', readOnly: readOnly || false }}
        value={code}
      />
    </Flex>
  );
}
