import { ConnectError, PromiseClient } from "@bufbuild/connect-web";
import {
  Alert,
  AlertDescription,
  AlertIcon,
  Box,
  Button,
  CloseButton,
  Code,
  Container,
  Heading,
  HStack,
  IconButton,
  Modal,
  ModalBody,
  ModalCloseButton,
  ModalContent,
  ModalFooter,
  ModalHeader,
  ModalOverlay,
  Skeleton,
  Stack,
  Table,
  Tbody,
  Td,
  Text,
  Th,
  Thead,
  Tr,
  useColorModeValue,
  useDisclosure,
} from "@chakra-ui/react";
import { useEffect, useState } from "react";
import { FiEdit2, FiInfo, FiXCircle } from "react-icons/fi";
import { useLinkClickHandler } from "react-router-dom";
import { ApiGrpc } from "../../gen/api_connectweb";
import { DeleteSinkReq, GetSinksReq, Sink } from "../../gen/api_pb";
import { stringifyBigint } from "../../lib/util";
import { ApiClient } from "../../main";

interface SinksState {
  sinks: Array<Sink> | null;
}

interface ColumnDef {
  name: string;
  accessor: (s: Sink) => string;
}

const columns: Array<ColumnDef> = [
  {
    name: "name",
    accessor: s => s.name,
  },
  {
    name: "type",
    accessor: s => s.sinkType.case!,
  },
  {
    name: "format",
    accessor: _ => "json",
  },
  {
    name: "consumers",
    accessor: _ => "-",
  },
  {
    name: "status",
    accessor: _ => "-",
  },
  {
    name: "data rate",
    accessor: _ => "-",
  },
];

function SinkTable({ client }: { client: ApiClient }) {
  const [state, setState] = useState<SinksState>({ sinks: null });

  const [message, setMessage] = useState<string | null>();
  const [isError, setIsError] = useState<boolean>(false);

  const [selected, setSelected] = useState<Sink | null>(null);

  const { isOpen, onOpen, onClose } = useDisclosure();


  useEffect(() => {
    const fetchData = async () => {
      const resp = await (await client()).getSinks(new GetSinksReq({}));

      setState({ sinks: resp.sinks });
    };

    fetchData();
  }, [message]);

  const deleteSink = async (sink: Sink) => {
    try {
      await (
        await client()
      ).deleteSink(
        new DeleteSinkReq({
          name: sink.name,
        })
      );
      setMessage(`Sink ${sink.name} successfully deleted`);
    } catch (e) {
      setIsError(true);
      if (e instanceof ConnectError) {
        setMessage(e.rawMessage);
      } else {
        setMessage("Something went wrong");
      }
    }
  };

  const onMessageClose = () => {
    setMessage(null);
    setIsError(false);
  };

  let messageBox = null;
  if (message != null) {
    messageBox = (
      <Alert status={isError ? "error" : "success"} width="100%">
        <AlertIcon />
        <AlertDescription flexGrow={1}>{message}</AlertDescription>
        <CloseButton alignSelf="flex-end" right={-1} top={-1} onClick={onMessageClose} />
      </Alert>
    );
  }

  return (
    <Stack spacing={2}>
      <Modal size="xl" onClose={onClose} isOpen={isOpen} isCentered>
        <ModalOverlay />
        <ModalContent>
          <ModalHeader>Sink {selected?.name}</ModalHeader>
          <ModalCloseButton />
          <ModalBody>
            <Code colorScheme="black" width="100%" p={4}>
              <pre>{stringifyBigint(selected?.sinkType.value, 2)}</pre>
            </Code>
          </ModalBody>
          <ModalFooter>
            <Button onClick={onClose}>Close</Button>
          </ModalFooter>
        </ModalContent>
      </Modal>
      {messageBox}
      <Table>
        <Thead>
          <Tr>
            {columns.map(c => {
              return (
                <Th key={c.name}>
                  <Text>{c.name}</Text>
                </Th>
              );
            })}
            <Th></Th>
          </Tr>
        </Thead>
        <Tbody>
          {state.sinks?.flatMap(sink => (
            <Tr key={sink.name}>
              {columns.map(column => (
                <Td key={sink.name + column.name}>{column.accessor(sink)}</Td>
              ))}

              <Td textAlign="right">
                <IconButton
                  icon={<FiInfo fontSize="1.25rem" />}
                  variant="ghost"
                  aria-label="View sink configuration"
                  onClick={() => {
                    setSelected(sink);
                    onOpen();
                  }}
                  title="Info"
                />
                <IconButton
                  icon={<FiXCircle fontSize="1.25rem" />}
                  variant="ghost"
                  aria-label="Delete sink"
                  onClick={() => deleteSink(sink)}
                  title="Delete"
                />
              </Td>
            </Tr>
          ))}
        </Tbody>
      </Table>
    </Stack>
  );
}

export function Sinks({ client }: { client: ApiClient }) {
  return (
    <Container py="8" flex="1">
      <Stack spacing={{ base: "8", lg: "6" }}>
        <Stack
          spacing="4"
          direction={{ base: "column", lg: "row" }}
          justify="space-between"
          align={{ base: "start", lg: "center" }}
        >
          <Stack spacing="1">
            <Heading size="sm" fontWeight="medium">
              Sinks
            </Heading>
          </Stack>
          <HStack spacing="3">
            <Button variant="primary" onClick={useLinkClickHandler("/sinks/new")}>
              Create Sink
            </Button>
          </HStack>
        </Stack>
        <Box
          bg="bg-surface"
          boxShadow={{ base: "none", md: useColorModeValue("sm", "sm-dark") }}
          borderRadius="lg"
        >
          <Stack spacing={{ base: "5", lg: "6" }}>
            <SinkTable client={client} />
          </Stack>
        </Box>
      </Stack>
    </Container>
  );
}
