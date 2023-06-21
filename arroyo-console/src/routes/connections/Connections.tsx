import { ConnectError } from '@bufbuild/connect-web';
import {
  Container,
  Stack,
  Heading,
  HStack,
  Button,
  Box,
  useColorModeValue,
  IconButton,
  Table,
  Tbody,
  Td,
  Th,
  Thead,
  Text,
  Tr,
  Icon,
  Code,
  Alert,
  AlertDescription,
  AlertIcon,
  CloseButton,
  Modal,
  ModalOverlay,
  ModalContent,
  ModalHeader,
  ModalCloseButton,
  ModalBody,
  ModalFooter,
} from '@chakra-ui/react';
import { useEffect, useState } from 'react';
import { FaGlobeAmericas, FaStream } from 'react-icons/fa';
import { FiInfo, FiXCircle } from 'react-icons/fi';
import { SiApachekafka } from 'react-icons/si';
import { useLinkClickHandler } from 'react-router-dom';
import { Connection, DeleteConnectionReq, GetConnectionsReq, ConnectionTable, TableType, Format } from '../../gen/api_pb';
import { ApiClient } from '../../main';
import { useConnectionTables } from '../../lib/data_fetching';

interface ColumnDef {
  name: string;
  accessor: (s: ConnectionTable) => JSX.Element;
}

const icons = {
  kafka: SiApachekafka,
  kinesis: FaStream,
  http: FaGlobeAmericas,
};

const format = (f: Format) => {
  switch (f) {
    case Format.AvroFormat:
      return 'Avro';
    case Format.JsonFormat:
      return 'JSON';
    case Format.ProtobufFormat:
      return 'Protobuf';
    case Format.RawStringFormat:
      return 'Rsaw';
    default:
      return 'unknown';
  }
}

const columns: Array<ColumnDef> = [
  {
    name: 'name',
    accessor: s => <Text>{s.name}</Text>,
  },
  {
    name: 'connector',
    accessor: s => <Text>{s.connector}</Text>,
  },
  {
    name: 'table type',
    accessor: s => <Text>{TableType[s.tableType].toLowerCase()}</Text>,
  },
  {
    name: 'format',
    accessor: s => <Text>{format(s.schema!.format)}</Text>
  },
  {
    name: "pipelines",
    accessor: s => <Text>1</Text>
  }
];

function ConnectionTableTable({ client }: { client: ApiClient }) {
  const [message, setMessage] = useState<string | null>();
  const [isError, setIsError] = useState<boolean>(false);
  const [selected, setSelected] = useState<ConnectionTable | null>(null);
  const {connectionTables, connectionTablesLoading} = useConnectionTables(client);

  const deleteTable = async (connection: ConnectionTable) => {
    try {
      await (
        await client()
      ).deleteConnection(
        new DeleteConnectionReq({
          name: connection.name,
        })
      );
      setMessage(`Connection ${connection.name} successfully deleted`);
    } catch (e) {
      setIsError(true);
      if (e instanceof ConnectError) {
        setMessage(e.rawMessage);
      } else {
        setMessage('Something went wrong');
      }
    }
  };

  const onClose = () => {
    setMessage(null);
    setIsError(false);
  };

  let messageBox = null;
  if (message != null) {
    messageBox = (
      <Alert status={isError ? 'error' : 'success'} width="100%">
        <AlertIcon />
        <AlertDescription flexGrow={1}>{message}</AlertDescription>
        <CloseButton alignSelf="flex-end" right={-1} top={-1} onClick={onClose} />
      </Alert>
    );
  }

  return (
    <Stack spacing={2}>
      <Modal size="2xl" onClose={() => setSelected(null)} isOpen={selected != null} isCentered>
        <ModalOverlay />
        <ModalContent>
          <ModalHeader>Connection {selected?.name}</ModalHeader>
          <ModalCloseButton />
          <ModalBody>
            <Code colorScheme="black" width="100%" p={4}>
              {/* toJson -> parse -> stringify to get around the inabilityof JSON.stringify to handle BigInt */}
              <pre>{JSON.stringify(JSON.parse(selected?.config || '{}'), null, 2)}</pre>
            </Code>
          </ModalBody>
          <ModalFooter>
            <Button onClick={() => setSelected(null)}>Close</Button>
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
          {connectionTables?.map(table => (
            <Tr key={table.name}>
              {columns.map(column => (
                <Td key={table.name + column.name}>{column.accessor(table)}</Td>
              ))}

              <Td textAlign={"right"}>
                <IconButton
                  icon={<FiInfo fontSize="1.25rem" />}
                  variant="ghost"
                  aria-label="View config"
                  onClick={() => setSelected(table)}
                />

                <IconButton
                  icon={<FiXCircle fontSize="1.25rem" />}
                  variant="ghost"
                  aria-label="Delete connection"
                  onClick={() => deleteTable(table)}
                />
              </Td>
            </Tr>
          ))}
        </Tbody>
      </Table>
    </Stack>
  );
}

export function Connections({ client }: { client: ApiClient }) {
  return (
    <Container py="8" flex="1">
      <Stack spacing={{ base: '8', lg: '6' }}>
        <Stack
          spacing="4"
          direction={{ base: 'column', lg: 'row' }}
          justify="space-between"
          align={{ base: 'start', lg: 'center' }}
        >
          <Stack spacing="1">
            <Heading size="sm" fontWeight="medium">
              Connections
            </Heading>
          </Stack>
          <HStack spacing="3">
            <Button variant="primary" onClick={useLinkClickHandler('/connections/new')}>
              Create Connection
            </Button>
          </HStack>
        </Stack>
        <Box
          bg="bg-surface"
          boxShadow={{ base: 'none', md: useColorModeValue('sm', 'sm-dark') }}
          borderRadius="lg"
        >
          <Stack spacing={{ base: '5', lg: '6' }}>
            <ConnectionTableTable client={client} />
          </Stack>
        </Box>
      </Stack>
    </Container>
  );
}
