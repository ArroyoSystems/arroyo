import Editor from '@monaco-editor/react';
import React, { Dispatch } from 'react';
import { Flex } from '@chakra-ui/react';

export function CodeEditor({
  code,
  setCode,
  readOnly,
  language,
}: {
  code: string;
  setCode?: Dispatch<string>;
  readOnly?: boolean;
  language?: string;
}) {
  const onChange = (value: string | undefined) => {
    if (setCode != null) {
      setCode(value || '');
    }
  };

  return (
    <Flex py={5} pr={5} flex={1}>
      <Editor
        defaultLanguage={language || 'sql'}
        onChange={onChange}
        theme="vs-dark"
        options={{ minimap: { enabled: false }, wordWrap: 'on', readOnly: readOnly || false }}
        value={code}
      />
    </Flex>
  );
}
