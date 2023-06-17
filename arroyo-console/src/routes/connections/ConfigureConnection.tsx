import { JSONSchema7 } from 'json-schema';
import { Connection, Connector, CreateConnectionReq } from '../../gen/api_pb';
import { ApiClient } from '../../main';
import {
  Button,
  HStack,
  Heading,
  Modal,
  ModalBody,
  ModalCloseButton,
  ModalContent,
  ModalHeader,
  ModalOverlay,
  Select,
  Spacer,
  Stack,
  Text,
  useDisclosure,
} from '@chakra-ui/react';
import { useState } from 'react';
import { JsonForm } from './JsonForm';
import { ConnectError } from '@bufbuild/connect-web';
import { AddIcon } from '@chakra-ui/icons';
import { useConnections } from '../../lib/data_fetching';
import { CreateConnectionState } from './CreateConnection';

const ClusterEditor = ({
  connector,
  connections,
  connectionId,
  setConnectionId,
  schema,
  client,
  addCluster,
}: {
  connector: string;
  connections: Array<Connection>;
  connectionId: string | null;
  setConnectionId: (id: string) => void;
  schema: JSONSchema7;
  client: ApiClient;
  addCluster: (c: Connection) => void;
}) => {
  const { isOpen, onOpen, onClose } = useDisclosure();
  const [error, setError] = useState<string | null>(null);

  const editor = (
    <JsonForm
      schema={schema}
      hasName={true}
      error={error}
      onSubmit={async data => {
        setError(null);
        try {
          const resp = await (
            await client()
          ).createConnection(
            new CreateConnectionReq({
              name: data.name,
              connector: connector,
              config: JSON.stringify(data),
            })
          );

          addCluster(resp.connection!);
          onClose();
        } catch (e) {
          if (e instanceof ConnectError) {
            setError(e.rawMessage);
          } else {
            setError('Something went wrong: ' + e);
          }
        }
      }}
    />
  );

  return (
    <>
      <Stack borderWidth="1px" borderColor={'gray.500'} padding={4} borderRadius={10} maxW={800}>
        <Heading size={'xx-small'}>Cluster config</Heading>

        <Text fontSize={'sm'}>Select an existing {connector} cluster or create a new one</Text>

        <Select
          placeholder="Choose cluster"
          value={connectionId || undefined}
          onChange={c => setConnectionId(c.target.value)}
        >
          {connections
            .filter(c => c.connector == connector)
            .map(c => (
              <option key={c.id} value={c.id}>
                {c.name}
              </option>
            ))}
        </Select>
        <HStack>
          <Spacer />
          <Button onClick={() => onOpen()}>
            <AddIcon w={3} h={3} mr={2} />
            Create new
          </Button>
        </HStack>
      </Stack>
      <Modal isOpen={isOpen} onClose={onClose} size={'2xl'}>
        <ModalOverlay />
        <ModalContent>
          <ModalHeader>Create {connector} connection</ModalHeader>
          <ModalCloseButton />
          <ModalBody p={4} px={7}>
            {editor}
          </ModalBody>
        </ModalContent>
      </Modal>
    </>
  );
};

export const ConfigureConnection = ({
  client,
  connector,
  onSubmit,
  state,
  setState,
}: {
  client: ApiClient;
  connector: Connector;
  onSubmit: () => void;
  state: CreateConnectionState;
  setState: (s: CreateConnectionState) => void;
}) => {
  let { connections, connectionsLoading, mutateConnections } = useConnections(client);

  if (connectionsLoading) {
    return <></>;
  }

  return (
    <Stack spacing={8}>
      {connector?.connectionConfig && (
        <ClusterEditor
          client={client}
          connectionId={state.connectionId}
          setConnectionId={(c) => setState({ ...state, connectionId: c })}
          connector={connector.id}
          connections={connections!}
          schema={JSON.parse(connector.connectionConfig)}
          addCluster={c => {
            mutateConnections([...connections!, c]);
            setState({ ...state, connectionId: c.id });
          }}
        />
      )}

      <Stack spacing="4" maxW={800}>
        <JsonForm
          schema={JSON.parse(connector!.tableConfig)}
          initial={state.table || {}}
          onSubmit={async table => {
            setState({ ...state, table: table })
            onSubmit();
          }}
          error={null}
          button={'Next'}
        />
      </Stack>
    </Stack>
  );
};
