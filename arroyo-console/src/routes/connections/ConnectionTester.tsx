import { Dispatch, useRef, useState } from 'react';
import { CreateConnectionState } from './CreateConnection';
import {
  Alert,
  AlertDescription,
  AlertDialog,
  AlertDialogBody,
  AlertDialogContent,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogOverlay,
  AlertIcon,
  AlertTitle,
  Box,
  Button,
  FormControl,
  FormHelperText,
  FormLabel,
  Input,
  Spinner,
  Stack,
  StackDivider,
  Text,
  useDisclosure,
} from '@chakra-ui/react';
import { useNavigate } from 'react-router-dom';
import {
  ConnectionTablePost,
  Connector,
  post,
  TestSourceMessage,
  useConnectionTableTest,
} from '../../lib/data_fetching';
import { formatError } from '../../lib/util';

export function ConnectionTester({
  connector,
  state,
  setState,
}: {
  connector: Connector;
  state: CreateConnectionState;
  setState: Dispatch<CreateConnectionState>;
}) {
  const [testing, setTesting] = useState<boolean>(false);
  const [messages, setMessages] = useState<Array<TestSourceMessage>>([]);
  const { isOpen, onOpen, onClose } = useDisclosure();
  const cancelRef = useRef<any>();
  const [touched, setTouched] = useState<boolean>(false);
  const [error, setError] = useState<{ title: string; body: string } | null>(null);
  const navigate = useNavigate();

  const done = messages.length > 0 && messages[messages?.length - 1].done;
  const errored = messages.find(m => m.error) != null;

  let config = JSON.parse(JSON.stringify(state.table));
  delete config.__meta;

  const isValidSQLTableName = (name: string | undefined) => {
    // This is a very basic check and may not cover all cases.
    // Update this according to your actual validation rules.
    return name && /^[_a-zA-Z][a-zA-Z0-9_]*$/.test(name);
  };

  const sseHandler = (event: TestSourceMessage) => {
    messages.push(event);
    setMessages(messages);
  };

  const createRequest: ConnectionTablePost = {
    name: state.name!,
    connector: connector.id,
    connectionProfileId: state.connectionProfileId,
    config: config,
    schema: state.schema || undefined,
  };

  const onClickTest = async () => {
    setTesting(true);
    setError(null);

    if (!testing) {
      await useConnectionTableTest(sseHandler, createRequest);
      setTesting(false);
    }
  };

  const submit = async () => {
    setError(null);
    const { error } = await post('/v1/connection_tables', { body: createRequest });
    if (error) {
      setError({ title: 'Failed to create connection', body: formatError(error) });
    } else {
      navigate('/connections');
    }
  };

  const onClickContinue = async () => {
    if (errored) {
      onOpen();
    } else {
      submit();
    }
  };

  let messageBox = null;
  if (messages.length) {
    messageBox = (
      <Box bg="bg-surface">
        <Stack divider={<StackDivider />} spacing="0">
          {messages.map((m, i) => {
            let status: 'error' | 'success' | 'info' = m.error
              ? 'error'
              : m.done
              ? 'success'
              : 'info';
            return (
              <Alert key={i} status={status}>
                <AlertIcon />
                <AlertDescription>{m.message}</AlertDescription>
              </Alert>
            );
          })}
        </Stack>
      </Box>
    );
  }

  return (
    <>
      {error && (
        <Alert status="error">
          <AlertIcon />
          <AlertTitle>{error.title}</AlertTitle>
          <AlertDescription>{error.body}</AlertDescription>
        </Alert>
      )}
      <Stack spacing={8} maxW={'md'}>
        <FormControl isInvalid={touched && (state.name === '' || !isValidSQLTableName(state.name))}>
          <FormLabel>Connection Name</FormLabel>
          <Input
            type="text"
            value={state.name || ''}
            onChange={v => {
              setTouched(true);
              setState({ ...state, name: v.target.value });
            }}
          />
          <FormHelperText>
            The connection will used in SQL using this name; it must be a {'\u00A0'}
            <dfn title="Names must start with a letter or _, contain only letters, numbers, and _s, and have fewer than 63 characters">
              valid SQL table name
            </dfn>
            <Text
              mt={2}
              style={{
                color:
                  touched && (!state.name || !isValidSQLTableName(state.name)) ? 'red' : 'inherit',
              }}
            >
              {touched && state.name === '' && "Table Name can't be empty."}
              {touched &&
                state.name !== '' &&
                !isValidSQLTableName(state.name) &&
                'Table Name is not a valid SQL table name.'}
            </Text>
          </FormHelperText>
        </FormControl>

        <Text>Before creating the connection, we can validate that it is configured properly.</Text>

        <Button
          variant="primary"
          isDisabled={testing || state.name === '' || !isValidSQLTableName(state.name)}
          onClick={onClickTest}
        >
          Test Connection
        </Button>

        {messageBox}

        {testing && !done && (
          <Box>
            <Spinner />
          </Box>
        )}

        {done && (
          <Button colorScheme={errored ? 'red' : 'green'} onClick={onClickContinue}>
            Create
          </Button>
        )}

        <AlertDialog isOpen={isOpen} leastDestructiveRef={cancelRef} onClose={onClose}>
          <AlertDialogOverlay>
            <AlertDialogContent>
              <AlertDialogHeader fontSize="lg" fontWeight="bold">
                Validation failed
              </AlertDialogHeader>

              <AlertDialogBody>
                We were not able to validate that the connection is correctly configured. You may
                continue creating it, but may encounter issues when using it in a query.
              </AlertDialogBody>

              <AlertDialogFooter>
                <Button ref={cancelRef} onClick={onClose}>
                  Go Back
                </Button>
                <Button
                  colorScheme="red"
                  onClick={() => {
                    onClose();
                    submit();
                  }}
                  ml={3}
                >
                  Create
                </Button>
              </AlertDialogFooter>
            </AlertDialogContent>
          </AlertDialogOverlay>
        </AlertDialog>
      </Stack>
    </>
  );
}
