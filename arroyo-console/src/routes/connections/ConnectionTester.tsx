import { Dispatch, useRef, useState } from 'react';
import { CreateConnectionState } from './CreateConnection';
import { ApiClient } from '../../main';
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
import {
  Connector,
  CreateConnectionTableReq,
  TestSourceMessage,
} from '../../gen/api_pb';
import { ConnectError } from '@bufbuild/connect-web';
import { useNavigate } from 'react-router-dom';

export function ConnectionTester({
  connector,
  state,
  setState,
  client,
}: {
  connector: Connector;
  state: CreateConnectionState;
  setState: Dispatch<CreateConnectionState>;
  client: ApiClient;
}) {
  const [testing, setTesting] = useState<boolean>(false);
  const [messages, setMessages] = useState<Array<TestSourceMessage> | null>(null);
  const { isOpen, onOpen, onClose } = useDisclosure();
  const cancelRef = useRef<any>();
  const [touched, setTouched] = useState<boolean>(false);
  const [error, setError] = useState<{ title: string; body: string } | null>(null);
  const navigate = useNavigate();

  const done = messages != null && messages.length > 0 && messages[messages?.length - 1].done;
  const errored = messages != null && messages.find(m => m.error) != null;

  let config = JSON.parse(JSON.stringify(state.table));
  delete config.__meta;

  const createRequest = new CreateConnectionTableReq({
    name: state.name,
    connector: connector.id,
    connectionId: state.connectionId || undefined,
    config: JSON.stringify(config),
    schema: state.schema || undefined,
  });

  const onClickTest = async () => {
    setTesting(true);
    setError(null);
    console.log('config', state);
    if (!testing) {
      try {
        let m: Array<TestSourceMessage> = [];
        setMessages(m);
        for await (const res of (await client()).testConnectionTable(createRequest)) {
          m = [...m, res];
          setMessages(m);
          await new Promise(r => setTimeout(r, 300));
        }
      } catch (e) {
        if (e instanceof ConnectError) {
          setError({ title: 'Failed to test connection', body: e.rawMessage });
        } else {
          setError({
            title: 'Failed to test connection',
            body: 'Something went wrong... try again',
          });
        }
      }
      setTesting(false);
    }
  };

  const submit = async () => {
    setError(null);
    try {
      await (await client()).createConnectionTable(createRequest);
      navigate('/connections');
    } catch (e) {
      if (e instanceof ConnectError) {
        setError({ title: 'Failed to create connection', body: e.rawMessage });
      } else {
        setError({
          title: 'Failed to create connection',
          body: 'Something went wrong... try again',
        });
      }
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
  if (messages != null) {
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
        <FormControl>
          <FormLabel>Connection Name</FormLabel>
          <Input
            isInvalid={touched && state.name == ''}
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
          </FormHelperText>
        </FormControl>

        <Text>Before creating the connection, we can validate that it is configured properly.</Text>

        <Button variant="primary" isDisabled={testing} onClick={onClickTest}>
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
