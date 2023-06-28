import { Dispatch, useEffect, useRef, useState } from 'react';
import { CreateConnectionState } from './CreateConnection';
import { ApiClient } from '../../main';
import { Alert, AlertIcon, Box, Button, List, ListItem, Stack } from '@chakra-ui/react';
import * as monaco from 'monaco-editor/esm/vs/editor/editor.api';
import { ConnectionSchema, TestSchemaReq } from '../../gen/api_pb';

export function JsonSchemaEditor({
  state,
  setState,
  next,
  client,
}: {
  state: CreateConnectionState;
  setState: Dispatch<CreateConnectionState>;
  next: () => void;
  client: ApiClient;
}) {
  const [editor, setEditor] = useState<monaco.editor.IStandaloneCodeEditor | null>(null);
  const monacoEl = useRef(null);
  const created = useRef(false);
  const [errors, setErrors] = useState<Array<string> | null>(null);
  const [testing, setTesting] = useState<boolean>(false);
  const [tested, setTested] = useState<string | undefined>();

  const valid = tested == editor?.getValue() && errors?.length == 0;

  const testSchema = async () => {
    setTesting(true);
    setErrors(null);
    try {
      let req = new TestSchemaReq({ schema: state.schema! });
      let resp = await (await client()).testSchema(req);

      setErrors(resp.errors);
    } catch (e) {
      setErrors(['Something went wrong... try again']);
    }

    setTested(editor?.getValue());
    setTesting(false);
  };

  let errorBox = null;
  if (errors != null) {
    if (errors.length == 0) {
      errorBox = (
        <Box>
          <Alert status="success">
            <AlertIcon />
            The schema is valid
          </Alert>
        </Box>
      );
    } else {
      errorBox = (
        <Box>
          <Alert status="error">
            <AlertIcon />
            <List>
              {errors.map(e => (
                <ListItem key={e}>{e}</ListItem>
              ))}
            </List>
          </Alert>
        </Box>
      );
    }
  }

  useEffect(() => {
    if (monacoEl && !editor && !created.current) {
      let e = monaco.editor.create(monacoEl.current!, {
        value: state.schema?.definition?.value,
        language: 'json',
        theme: 'vs-dark',
        minimap: {
          enabled: false,
        },
      });

      e?.getModel()?.onDidChangeContent(_ => {
        setState({
          ...state,
          schema: new ConnectionSchema({
            ...state.schema,
            definition: { case: 'jsonSchema', value: e.getValue() },
          }),
        });
      });

      created.current = true;
      setEditor(e);
    }

    return () => editor?.dispose();
  }, []);

  return (
    <Stack spacing={4}>
      <Box marginTop={5} width="100%">
        <div className="editor" ref={monacoEl}></div>
      </Box>

      {errorBox}

      {valid ? (
        <Button width={150} colorScheme="green" onClick={next}>
          Next
        </Button>
      ) : (
        <Button width={150} variant="primary" isLoading={testing} onClick={testSchema}>
          Validate
        </Button>
      )}
    </Stack>
  );
}
