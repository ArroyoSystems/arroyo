import { Dispatch, useState } from 'react';
import { CreateConnectionState } from './CreateConnection';
import { ApiClient } from '../../main';
import { ConnectError } from '@bufbuild/connect-web';
import {
  Alert,
  AlertIcon,
  Box,
  Button,
  Code,
  FormControl,
  FormHelperText,
  FormLabel,
  Heading,
  Input,
  Stack,
} from '@chakra-ui/react';
import { ConfluentSchemaReq, ConnectionSchema, FormatOptions } from '../../gen/api_pb';

export function ConfluentSchemaEditor({
  state,
  setState,
  client,
  next,
}: {
  state: CreateConnectionState;
  setState: Dispatch<CreateConnectionState>;
  client: ApiClient;
  next: () => void;
}) {
  const [host, setHost] = useState<string>('');
  const [schema, setSchema] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState<boolean>(false);

  const topic = state.table.topic;

  const fetchSchema = async () => {
    setError(null);
    try {
      setLoading(true);
      const resp = await (
        await client()
      ).getConfluentSchema(
        new ConfluentSchemaReq({
          endpoint: host,
          topic: topic,
        })
      );

      setSchema(resp.schema);

      setState({
        ...state,
        schema: new ConnectionSchema({
          ...state.schema,
          definition: { case: 'jsonSchema', value: resp.schema },
          formatOptions: new FormatOptions({
            confluentSchemaRegistry: true,
          }),
        }),
      });
    } catch (e) {
      if (e instanceof ConnectError) {
        setError(e.rawMessage);
      } else {
        setError('Something went wrong... try again');
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
      </Stack>
    );
  }

  return (
    <Stack spacing={4} maxW="lg">
      <FormControl>
        <FormLabel>Schema Registry Endpoint</FormLabel>
        <Input
          type="text"
          placeholder="http://localhost:8081"
          value={host}
          onChange={e => setHost(e.target.value)}
        />
        <FormHelperText>Provide the endpoint for your Confluent Schema Registry</FormHelperText>
      </FormControl>

      {errorBox}

      <Button variant="primary" isDisabled={host == ''} isLoading={loading} onClick={fetchSchema}>
        Fetch Schema
      </Button>

      {successBox}

      {schema != null && (
        <Button colorScheme="green" onClick={() => next()}>
          Continue
        </Button>
      )}
    </Stack>
  );
}
