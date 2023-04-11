import { PromiseClient } from "@bufbuild/connect-web";
import {
  Stack,
  Heading,
  Text,
  Box,
  Code,
  Card,
  CardBody,
  CardHeader,
  StackDivider,
  Button,
  Spinner,
  Alert,
  AlertIcon,
  AlertDescription,
  useDisclosure,
  AlertDialog,
  AlertDialogBody,
  AlertDialogContent,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogOverlay,
} from "@chakra-ui/react";
import { config } from "process";
import { Dispatch, useRef, useState } from "react";
import { ApiGrpc } from "../../gen/api_connectweb";
import {
  CreateSourceReq,
  JsonSchemaDef,
  TestSchemaResp,
  TestSourceMessage,
} from "../../gen/api_pb";
import { ApiClient } from "../../main";

export function TestSource({
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
  const [testing, setTesting] = useState<boolean>(false);
  const [messages, setMessages] = useState<Array<TestSourceMessage> | null>(null);
  const { isOpen, onOpen, onClose } = useDisclosure();
  const cancelRef = useRef<any>();

  const schemaDisplay = new Map([
    [
      "jsonSchema",
      <Box>
        <Text fontWeight="bold">Json Schema</Text>
        <Code maxHeight={250} overflowY="scroll" whiteSpace="pre" width="100%">
          {JSON.stringify(
            JSON.parse((state.schema?.schema.value as JsonSchemaDef).jsonSchema),
            null,
            2
          )}
        </Code>
      </Box>,
    ],
  ]);

  const done = messages != null && messages.length > 0 && messages[messages?.length - 1].done;
  const errored = messages != null && messages.find(m => m.error) != null;

  const onClickTest = async () => {
    setTesting(true);
    if (!testing) {
      try {
        let m: Array<TestSourceMessage> = [];
        setMessages(m);
        for await (const res of (await client()).testSource(state)) {
          m = [...m, res];
          setMessages(m);
          await new Promise(r => setTimeout(r, 300));
        }
      } catch (e) {
        console.log("Request failed", e);
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
            let status: "error" | "success" | "info" = m.error
              ? "error"
              : m.done
              ? "success"
              : "info";
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
    <Stack spacing={5} maxWidth={600}>
      <Heading size="sm">Test Source</Heading>
      <Text>
        Before creating the source, we can test the connection and validate that we are able to read
        the messages according to the schema you've defined.
      </Text>

      <Text>To review, here's the source that you've configured:</Text>

      <Card>
        <CardBody>
          <Stack divider={<StackDivider />} spacing={2}>
            <Heading size="xs">{state.typeOneof.case} source</Heading>
            <Box>
              <Text fontWeight="bold">Configuration</Text>
              <Code>{JSON.stringify(state.typeOneof.value)}</Code>
            </Box>
            {schemaDisplay.get(state.schema?.schema.case!)}
          </Stack>
        </CardBody>
      </Card>

      {!done ? (
        <Button variant="primary" isDisabled={testing} onClick={onClickTest}>
          Test Source
        </Button>
      ) : (
        <Button colorScheme={errored ? "red" : "green"} onClick={onClickContinue}>
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
              We were not able to validate that the source is correctly configured. You may continue
              creating it, but may encounter issues trying to query from it.
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
