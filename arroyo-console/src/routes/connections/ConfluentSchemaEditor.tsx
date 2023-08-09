import { Dispatch, useState } from 'react';
import { CreateConnectionState } from './CreateConnection';
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
import { get } from '../../lib/data_fetching';
import { formatError } from '../../lib/util';

export function ConfluentSchemaEditor({
  state,
  setState,
  next,
}: {
  state: CreateConnectionState;
  setState: Dispatch<CreateConnectionState>;
  next: () => void;
}) {
  const [endpoint, setEndpoint] = useState<string>('');
  const [schema, setSchema] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState<boolean>(false);

  const topic = state.table.topic;

  const fetchSchema = async () => {
    setError(null);
    setLoading(true);
    const { data, error } = await get('/v1/connection_tables/schemas/confluent', {
      params: {
        query: {
          endpoint,
          topic,
        },
      },
    });

    if (error) {
      setError(formatError(error));
    }

    if (data) {
      setSchema(data.schema);
      setState({
        ...state,
        schema: {
          ...state.schema,
          definition: { json_schema: data.schema },
          fields: [],
          format: { json: { confluentSchemaRegistry: true } },
        },
      });
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
          value={endpoint}
          onChange={e => setEndpoint(e.target.value)}
        />
        <FormHelperText>Provide the endpoint for your Confluent Schema Registry</FormHelperText>
      </FormControl>

      {errorBox}

      <Button
        variant="primary"
        isDisabled={endpoint == ''}
        isLoading={loading}
        onClick={fetchSchema}
      >
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
