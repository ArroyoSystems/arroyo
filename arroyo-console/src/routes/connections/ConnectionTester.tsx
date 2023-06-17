import { Dispatch, useRef, useState } from 'react';
import { CreateConnectionState } from './CreateConnection';
import { ApiClient } from '../../main';
import { Alert, AlertDescription, AlertDialog, AlertDialogBody, AlertDialogContent, AlertDialogFooter, AlertDialogHeader, AlertDialogOverlay, AlertIcon, Box, Button, Spinner, Stack, StackDivider, Text, useDisclosure } from '@chakra-ui/react';
import { Connector, CreateConnectionReq, CreateConnectionTableReq, TestSourceMessage } from '../../gen/api_pb';

export function ConnectionTester({
  connector,
  state,
  setState,
  next,
  client,
}: {
  connector: Connector;
  state: CreateConnectionState;
  setState: Dispatch<CreateConnectionState>;
  next: () => void;
  client: ApiClient;
}) {

  const [testing, setTesting] = useState<boolean>(false);
  const [messages, setMessages] = useState<Array<TestSourceMessage> | null>(null);
  const { isOpen, onOpen, onClose } = useDisclosure();
  const cancelRef = useRef<any>();

  const done = messages != null && messages.length > 0 && messages[messages?.length - 1].done;
  const errored = messages != null && messages.find(m => m.error) != null;

  const createRequest = new CreateConnectionTableReq({
    name: "__test_connection",
    connector: connector.id,
    connectionId: state.connectionId || undefined,
    config: JSON.stringify(state.table),
    schema: state.schema || undefined,
  })

  const onClickTest = async () => {
    setTesting(true);
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
        console.log('Request failed', e);
      }
    }
  };

  const onClickContinue = () => {
    if (errored) {
      onOpen();
    } else {
      next();
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
    <Stack spacing={4}>
      <Text>Before creating the connection, we can validate that it is configured properly.</Text>

      {!done ? (
        <Button variant="primary" isDisabled={testing} onClick={onClickTest}>
          Test Connection
        </Button>
      ) : (
        <Button colorScheme={errored ? 'red' : 'green'} onClick={onClickContinue}>
          Continue
        </Button>
      )}

      {messageBox}

      <Box>{testing && !done ? <Spinner /> : null}</Box>

      <AlertDialog isOpen={isOpen} leastDestructiveRef={cancelRef} onClose={onClose}>
        <AlertDialogOverlay>
          <AlertDialogContent>
            <AlertDialogHeader fontSize="lg" fontWeight="bold">
              Validation failed
            </AlertDialogHeader>

            <AlertDialogBody>
              We were not able to validate that the connection is correctly configured. You may continue
              creating it, but may encounter issues when using it in a query.
            </AlertDialogBody>

            <AlertDialogFooter>
              <Button ref={cancelRef} onClick={onClose}>
                Go Back
              </Button>
              <Button colorScheme="red" onClick={() => next()} ml={3}>
                Continue
              </Button>
            </AlertDialogFooter>
          </AlertDialogContent>
        </AlertDialogOverlay>
      </AlertDialog>
    </Stack>
  );
}
