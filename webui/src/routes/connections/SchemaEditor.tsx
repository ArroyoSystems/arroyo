import { Dispatch, useEffect, useRef, useState } from 'react';
import { CreateConnectionState } from './CreateConnection';
import {
  Alert,
  AlertIcon,
  Box,
  Button,
  Checkbox,
  FormControl,
  FormHelperText,
  List,
  ListItem,
  Stack,
} from '@chakra-ui/react';
import * as monaco from 'monaco-editor/esm/vs/editor/editor.api';
import { ConnectionSchema, post } from '../../lib/data_fetching';
import { formatError } from '../../lib/util';

export function SchemaEditor({
  state,
  setState,
  next,
  format,
}: {
  state: CreateConnectionState;
  setState: Dispatch<CreateConnectionState>;
  next: () => void;
  format: 'avro' | 'json';
}) {
  const [editor, setEditor] = useState<monaco.editor.IStandaloneCodeEditor | null>(null);
  const monacoEl = useRef(null);
  const created = useRef(false);
  const [errors, setErrors] = useState<Array<string> | null>(null);
  const [testing, setTesting] = useState<boolean>(false);
  const [tested, setTested] = useState<string | undefined>();
  const [rawDatum, setRawDatum] = useState<boolean>(false);

  const valid = tested == editor?.getValue() && errors?.length == 0;

  const testSchema = async () => {
    // if avro and raw datum, then we need to add the raw datum encoding
    if (format == 'avro' && rawDatum) {
      // @ts-ignore
      state.schema!.format['avro']!.rawDatums = rawDatum;
      // update the state
      setState({
        ...state,
        schema: state.schema,
      });
    }

    setTesting(true);
    setErrors(null);
    const { error } = await post('/v1/connection_tables/schemas/test', {
      body: state.schema!,
    });
    if (error) {
      setErrors([formatError(error)]);
    } else {
      setErrors([]);
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

  let avroOptions = null;
  if (format == 'avro') {
    avroOptions = (
      <Box maxW={'lg'}>
        <FormControl>
          <Checkbox
            onChange={e => {
              console.log('CHECKED = ', e.target.checked);
              setRawDatum(e.target.checked);
            }}
          >
            Raw datum encoding
          </Checkbox>
          <FormHelperText>
            This encoding should be used for streams composed of individual <i>avro datums</i>,
            rather than complete Avro documents with embedded schemas
          </FormHelperText>
        </FormControl>
      </Box>
    );
  }

  useEffect(() => {
    if (monacoEl && !editor && !created.current) {
      let e = monaco.editor.create(monacoEl.current!, {
        language: 'json',
        theme: 'vs-dark',
        minimap: {
          enabled: false,
        },
      });

      e?.getModel()?.onDidChangeContent(_ => {
        let schema: ConnectionSchema = {
          ...state.schema,
          fields: [],
          // @ts-ignore
          format: {},
          // @ts-ignore
          definition: {},
        };

        // @ts-ignore
        schema.format![format] = {};

        // @ts-ignore
        schema.definition![format + '_schema'] = e.getValue();

        setState({
          ...state,
          schema: schema,
        });
      });

      created.current = true;
      setEditor(e);
    }

    return () => editor?.dispose();
  }, []);

  return (
    <Stack spacing={4}>
      {avroOptions}
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
