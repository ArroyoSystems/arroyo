import { Dispatch } from 'react';
import { CreateConnectionState } from './CreateConnection';
import { Button, Stack, Text } from '@chakra-ui/react';

export function ConfluentSchemaEditor({
  state,
  setState,
  next,
}: {
  state: CreateConnectionState;
  setState: Dispatch<CreateConnectionState>;
  next: () => void;
}) {
  return (
    <Stack spacing={4} maxW="lg">
      <Text>Schemas will be loaded from the configured Confluent Schema Registry</Text>

      <Button colorScheme="green" onClick={() => next()}>
        Continue
      </Button>
    </Stack>
  );
}
