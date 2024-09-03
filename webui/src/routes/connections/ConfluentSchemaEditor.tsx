import { Dispatch } from 'react';
import { CreateConnectionState } from './CreateConnection';
import {
  Box,
  Button,
  FormControl,
  FormHelperText,
  FormLabel,
  Input,
  Stack,
  Text,
} from '@chakra-ui/react';

export function ConfluentSchemaEditor({
  state,
  setState,
  next,
}: {
  state: CreateConnectionState;
  setState: Dispatch<CreateConnectionState>;
  next: () => void;
}) {
  let formatEl = null;

  if (state.schema!.format!.protobuf !== undefined) {
    formatEl = (
      <Box maxW={'lg'}>
        <FormControl>
          <FormLabel>Message name</FormLabel>
          <Input
            // @ts-ignore
            value={state.schema!.format.protobuf!.messageName}
            onChange={e => {
              setState({
                ...state,
                schema: {
                  ...state.schema!,
                  format: {
                    ...state.schema!.format,
                    // @ts-ignore
                    protobuf: {
                      // @ts-ignore
                      ...state.schema!.format.protobuf,
                      messageName: e.target.value,
                    },
                  },
                },
              });
            }}
          />
          <FormHelperText>
            The name of the protobuf message for the data in this table
          </FormHelperText>
        </FormControl>
      </Box>
    );
  }

  return (
    <Stack spacing={4} maxW="lg">
      <Text>
        Schemas will be loaded from and written to the configured Confluent Schema Registry
      </Text>

      {formatEl}

      <Button colorScheme="green" onClick={() => next()}>
        Continue
      </Button>
    </Stack>
  );
}
