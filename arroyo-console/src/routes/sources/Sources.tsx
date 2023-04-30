//import './Sources.css';

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
import { FiEdit2, FiEye, FiInfo, FiXCircle } from "react-icons/fi";
import { useLinkClickHandler, useNavigate, Link } from "react-router-dom";
import { ApiGrpc } from "../../gen/api_connectweb";
import { DeleteSourceReq, GetSourcesReq, SourceDef } from "../../gen/api_pb";
import { ApiClient } from "../../main";

interface SourcesState {
  sources: Array<SourceDef> | null;
}

interface ColumnDef {
  name: string;
  accessor: (s: SourceDef) => string;
}

const columns: Array<ColumnDef> = [
  {
    name: "type",
    accessor: s => s.sourceType.case!,
  },
  {
    name: "format",
    accessor: s => s.schema?.schema.case!,
  },
  {
    name: "consumers",
    accessor: s => String(s.consumers),
  }
];


function SourceTable({ client }: { client: ApiClient }) {
  const [state, setState] = useState<SourcesState>({ sources: null });

  const [message, setMessage] = useState<string | null>();
  const [isError, setIsError] = useState<boolean>(false);

  const [selected, setSelected] = useState<SourceDef | null>(null);

  const { isOpen, onOpen, onClose } = useDisclosure();

  const navigate = useNavigate();

  useEffect(() => {
    const fetchData = async () => {
      const sources = await (await client()).getSources(new GetSourcesReq({}));

      setState({ sources: sources.sources });
    };

    fetchData();
  }, [message]);

  const deleteSource = async (source: SourceDef) => {
    try {
      await (
        await client()
      ).deleteSource(
        new DeleteSourceReq({
          name: source.name,
        })
      );
      setMessage(`Source ${source.name} successfully deleted`);
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
    <Stack spacing="2">
      <Modal size="xl" onClose={onClose} isOpen={isOpen} isCentered>
        <ModalOverlay />
        <ModalContent>
          <ModalHeader>Source {selected?.name}</ModalHeader>
          <ModalCloseButton />
          <ModalBody>
            <Code colorScheme="black" width="100%" p={4}>
              { /* toJson -> parse -> stringify to get around the inabilityof JSON.stringify to handle BigInt */}
              <pre>{JSON.stringify(JSON.parse(selected?.sourceType.value?.toJsonString() || "{}"), null, 2)}</pre>
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
            <Th key="name">
              <Text>name</Text>
            </Th>
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
          {state.sources?.flatMap(source => (
            <Tr key={source.name}>
              <Td key={source.name + "name"}>
                <Link to={`${source.id}`}>{source.name}</Link>
              </Td>
              {
                columns.map(column => (
                  <Td key={source.name + column.name}>{column.accessor(source)}</Td>
                ))
              }

              < Td textAlign="right" >
                <IconButton
                  icon={<FiInfo fontSize="1.25rem" />}
                  variant="ghost"
                  aria-label="View source configuration"
                  onClick={() => {
                    setSelected(source);
                    onOpen();
                  }}
                  title="Info"
                />
                <IconButton
                  icon={<FiXCircle fontSize="1.25rem" />}
                  variant="ghost"
                  aria-label="Delete source"
                  onClick={() => deleteSource(source)}
                  title="Delete"
                />
              </Td>
            </Tr>
          ))}
        </Tbody>
      </Table >
    </Stack >
  );
}

export function Sources({ client }: { client: ApiClient }) {

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
              Sources
            </Heading>
          </Stack>
          <HStack spacing="3">
            <Button variant="primary" onClick={useLinkClickHandler("/sources/new")}>
              Create Source
            </Button>
          </HStack>
        </Stack>
        <Box
          bg="bg-surface"
          boxShadow={{ base: "none", md: useColorModeValue("sm", "sm-dark") }}
          borderRadius="lg"
        >
          <Stack spacing={{ base: "5", lg: "6" }}>
            <SourceTable client={client} />
          </Stack>
        </Box>
      </Stack>
    </Container>
  );
}
