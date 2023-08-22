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
import React, { useState } from 'react';
import { FiInfo, FiXCircle } from 'react-icons/fi';
import { ConnectionTable, del, Format, useConnectionTables } from '../../lib/data_fetching';
import { useNavigate } from 'react-router-dom';
import { formatError } from '../../lib/util';
import { formatDate } from '../../lib/util';
import PaginatedContent from '../../components/PaginatedContent';

interface ColumnDef {
  name: string;
  accessor: (s: ConnectionTable) => JSX.Element;
}

const format = (f: Format | undefined) => {
  if (f?.json) {
    return 'JSON';
  } else if (f?.raw_string) {
    return 'RawString';
  } else if (f?.parquet) {
    return 'Parquet';
  } else if (f?.avro) {
    return 'Avro';
  } else {
    return 'Unknown';
  }
};

const columns: Array<ColumnDef> = [
  {
    name: 'name',
    accessor: s => <Text>{s.name}</Text>,
  },
  {
    name: 'created at',
    accessor: s => <Text>{formatDate(BigInt(s.createdAt))}</Text>,
  },
  {
    name: 'connector',
    accessor: s => <Text>{s.connector}</Text>,
  },
  {
    name: 'table type',
    accessor: s => <Text>{s.tableType}</Text>,
  },
  {
    name: 'format',
    accessor: s => <Text>{s.schema?.format ? format(s.schema.format!) : 'Built-in'}</Text>,
  },
  {
    name: 'pipelines',
    accessor: s => <Text>{s.consumers}</Text>,
  },
];

export function Connections() {
  const navigate = useNavigate();
  const [message, setMessage] = useState<string | null>();
  const [isError, setIsError] = useState<boolean>(false);
  const [selected, setSelected] = useState<ConnectionTable | null>(null);
  const {
    connectionTablePages,
    connectionTablesLoading,
    mutateConnectionTables,
    connectionTablesTotalPages,
    setConnectionTablesMaxPages,
  } = useConnectionTables(10);
  const [connectionTables, setConnectionTables] = useState<ConnectionTable[]>([]);

  const deleteTable = async (connection: ConnectionTable) => {
    const { error } = await del('/v1/connection_tables/{id}', {
      params: { path: { id: connection.id } },
    });
    mutateConnectionTables();
    if (error) {
      setIsError(true);
      setMessage(formatError(error));
    } else {
      setIsError(false);
      setMessage(`Connection ${connection.name} successfully deleted`);
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

  const configModal = (
    <Modal size="2xl" onClose={() => setSelected(null)} isOpen={selected != null} isCentered>
      <ModalOverlay />
      <ModalContent>
        <ModalHeader>Connection {selected?.name}</ModalHeader>
        <ModalCloseButton />
        <ModalBody>
          <Code colorScheme="black" width="100%" p={4}>
            {/* toJson -> parse -> stringify to get around the inabilityof JSON.stringify to handle BigInt */}
            <pre>{JSON.stringify(selected?.config)}</pre>
          </Code>
        </ModalBody>
        <ModalFooter>
          <Button onClick={() => setSelected(null)}>Close</Button>
        </ModalFooter>
      </ModalContent>
    </Modal>
  );

  const tableHeader = (
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
  );

  const tableBody = (
    <Tbody>
      {connectionTables?.map(table => (
        <Tr key={table.name}>
          {columns.map(column => (
            <Td key={table.name + column.name}>{column.accessor(table)}</Td>
          ))}

          <Td textAlign={'right'}>
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
  );

  const table = (
    <Box
      bg="bg-surface"
      boxShadow={{ base: 'none', md: useColorModeValue('sm', 'sm-dark') }}
      borderRadius="lg"
    >
      <Stack spacing={{ base: '5', lg: '6' }}>
        <Stack spacing={2}>
          {configModal}
          {messageBox}
          <Table>
            {tableHeader}
            {tableBody}
          </Table>
        </Stack>{' '}
      </Stack>
    </Box>
  );

  return (
    <Container py="8" flex="1">
      <Stack
        spacing={{
          base: '8',
          lg: '6',
        }}
      >
        <Stack
          spacing="4"
          direction={{
            base: 'column',
            lg: 'row',
          }}
          justify="space-between"
          align={{
            base: 'start',
            lg: 'center',
          }}
        >
          <Stack spacing="1">
            <Heading size="sm" fontWeight="medium">
              Connections
            </Heading>
          </Stack>
          <HStack spacing="3">
            <Button variant="primary" onClick={() => navigate('/connections/new')}>
              Create Connection
            </Button>
          </HStack>
        </Stack>
        <PaginatedContent
          pages={connectionTablePages}
          loading={connectionTablesLoading}
          totalPages={connectionTablesTotalPages}
          setMaxPages={setConnectionTablesMaxPages}
          setCurrentData={setConnectionTables}
          content={table}
        />
      </Stack>
    </Container>
  );
}
