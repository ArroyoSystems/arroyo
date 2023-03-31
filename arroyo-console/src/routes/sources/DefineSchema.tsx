import {
  Stack,
  Heading,
  Button,
  Tabs,
  TabPanel,
  TabList,
  Tab,
  Box,
  Spinner,
  ListItem,
  List,
  Alert,
  AlertIcon,
  FormControl,
  FormHelperText,
  FormLabel,
  Input,
  Code,
} from "@chakra-ui/react";
import { Dispatch, useEffect, useRef, useState } from "react";
import {
  ConfluentSchemaReq,
  CreateSourceReq,
  KafkaSourceConfig,
  SourceSchema,
} from "../../gen/api_pb";
import { RadioCard, RadioCardGroup } from "../../lib/RadioGroup";
import * as monaco from "monaco-editor/esm/vs/editor/editor.api";
import { ConnectError, PromiseClient } from "@bufbuild/connect-web";
import { ApiGrpc } from "../../gen/api_connectweb";
import { ApiClient } from "../../main";

export function ChooseSchemaType({
  state,
  setState,
  next,
}: {
  state: CreateSourceReq;
  setState: Dispatch<CreateSourceReq>;
  next: (step?: number) => void;
}) {
  const handleChange = (v: string) => {
    setState({
      ...state,
      /* @ts-ignore */
      schema: { schema: { case: v, value: {} } },
    });
  };

  return (
    <Stack spacing={10} maxWidth={400}>
      <Heading size="xs">Select schema type</Heading>
      <RadioCardGroup onChange={handleChange} value={state.schema?.schema.case || undefined}>
        <RadioCard value="jsonSchema">Json Schema</RadioCard>
        <RadioCard value="protobuf" isDisabled>
          Protobuf (coming soon)
        </RadioCard>
        <RadioCard value="avro" isDisabled>
          Avro (coming soon)
        </RadioCard>
        {state.typeOneof.case == "kafka" ? (
          <RadioCard value="confluentSchema">Confluent Schema Registry</RadioCard>
        ) : null}
      </RadioCardGroup>
      <Button variant="primary" onClick={() => next()}>
        Continue
      </Button>
    </Stack>
  );
}

function SchemaEditor({
  state,
  setState,
  language,
  field,
  next,
  client,
}: {
  state: CreateSourceReq;
  setState: Dispatch<CreateSourceReq>;
  language: string;
  field: string;
  next: (step?: number) => void;
  client: ApiClient;
}) {
  const [editor, setEditor] = useState<monaco.editor.IStandaloneCodeEditor | null>(null);
  const monacoEl = useRef(null);
  const created = useRef(false);
  const [errors, setErrors] = useState<Array<string> | null>(null);
  const [testing, setTesting] = useState<boolean>(false);
  const [tested, setTested] = useState<CreateSourceReq | null>(null);

  const testSchema = async () => {
    setTesting(true);
    try {
      let resp = await (await client()).testSchema(state);
      setErrors(resp.errors);
    } catch (e) {
      console.log("test failed", e);
      setErrors(["Something went wrong... try again"]);
    }
    setTested(state);

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
        /* @ts-ignore */
        value: state.schema?.schema.value[field],
        language: language,
        theme: "vs-dark",
        minimap: {
          enabled: false,
        },
      });

      e?.getModel()?.onDidChangeContent(_ => {
        let v: { [index: string]: string } = {};
        v[field] = e.getValue();
        setState(
          new CreateSourceReq({
            ...state,
            /* @ts-ignore */
            schema: new SourceSchema({ schema: { case: state.schema?.schema.case, value: v } }),
          })
        );
      });

      created.current = true;
      setEditor(e);
    }

    return () => editor?.dispose();
  }, []);

  return (
    <Stack>
      {errorBox}

      <Box marginTop={5} width="100%">
        <div className="editor" ref={monacoEl}></div>
      </Box>

      {errors?.length == 0 && tested == state ? (
        <Button width={150} variant="primary" onClick={() => next()}>
          Continue
        </Button>
      ) : (
        <Button width={150} variant="primary" isLoading={testing} onClick={testSchema}>
          Validate
        </Button>
      )}
    </Stack>
  );
}

function ConfluentSchema({
  state,
  setState,
  next,
  client,
}: {
  state: CreateSourceReq;
  setState: Dispatch<CreateSourceReq>;
  next: (step?: number) => void;
  client: ApiClient;
}) {
  const [host, setHost] = useState<string>("");
  const [schema, setSchema] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState<boolean>(false);

  const topic = (state.typeOneof.value as KafkaSourceConfig).topic;

  const fetchSchema = async () => {
    setError(null);
    try {
      setLoading(true);
      const resp = await (await client()).getConfluentSchema(
        new ConfluentSchemaReq({
          endpoint: host,
          /* @ts-ignore */
          topic: topic,
        })
      );

      setSchema(resp.schema);

      setState(
        new CreateSourceReq({
          ...state,
          schema: new SourceSchema({
            kafkaSchemaRegistry: true,
            schema: { case: "jsonSchema", value: { jsonSchema: resp.schema } },
          }),
        })
      );

      console.log(state);
    } catch (e) {
      if (e instanceof ConnectError) {
        setError(e.rawMessage);
      } else {
        setError("Something went wrong... try again");
      }
    }
    setLoading(false);
  };

  let errorBox = null;
  if (error != null) {
    errorBox = (
      <Box>
        <Alert status="error">
          <AlertIcon />
          {error}
        </Alert>
      </Box>
    );
  }

  let successBox = null;
  if (schema != null) {
    successBox = (
      <Stack spacing={4}>
        <Heading size="xs">Fetched schema for topic {topic}</Heading>
        <Code height={300} overflowY="scroll">
          <pre>{JSON.stringify(JSON.parse(schema), null, 2)}</pre>
        </Code>

        <Button variant="primary" onClick={() => next()}>
          Continue
        </Button>
      </Stack>
    );
  }

  return (
    <Stack spacing={4} maxW="md">
      {errorBox}

      <FormControl>
        <FormLabel>Schema Registry Endpoint</FormLabel>
        <Input type="text" value={host} onChange={e => setHost(e.target.value)} />
        <FormHelperText>Provide the endpoint for your Confluent Schema Registry</FormHelperText>
      </FormControl>

      <Button
        variant="primary"
        disabled={host == "" || schema != null}
        isLoading={loading}
        onClick={fetchSchema}
      >
        Fetch Schema
      </Button>

      {successBox}
    </Stack>
  );
}

export function DefineSchema({
  state,
  setState,
  next,
  client,
}: {
  state: CreateSourceReq;
  setState: Dispatch<CreateSourceReq>;
  next: (step?: number) => void;
  client: ApiClient;
}) {
  const types = new Map([
    [
      "jsonSchema",
      {
        name: "Json Schema",
        view: (
          <SchemaEditor
            state={state}
            setState={setState}
            language="json"
            field="jsonSchema"
            next={next}
            client={client}
          />
        ),
      },
    ],
    [
      "protobuf",
      {
        name: "Protobuf Schema",
        view: (
          <SchemaEditor
            state={state}
            setState={setState}
            language="protobuf"
            field="protobufSchema"
            next={next}
            client={client}
          />
        ),
        disabled: true
      },
    ],
    [
      "confluentSchema",
      {
        name: "Confluent Schema Registry",
        view: <ConfluentSchema state={state} setState={setState} next={next} client={client} />,
      },
    ],
  ]);

  const config = state.schema?.kafkaSchemaRegistry
    ? types.get("confluentSchema")
    : types.get(state.schema?.schema.case!);

  return (
    <Stack spacing={10}>
      <Heading size="xs">{config?.name}</Heading>
      {config?.view}
    </Stack>
  );
}
