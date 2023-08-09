import { JSONSchema7 } from 'json-schema';
import {
  Button,
  FormControl,
  FormErrorMessage,
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
import { AddIcon } from '@chakra-ui/icons';
import { ConnectionProfile, Connector, post, useConnectionProfiles } from '../../lib/data_fetching';
import { CreateConnectionState } from './CreateConnection';
import { formatError } from '../../lib/util';

const ClusterEditor = ({
  connector,
  connections,
  connectionId,
  setConnectionId,
  clusterError,
  schema,
  addConnectionProfile,
}: {
  connector: string;
  connections: Array<ConnectionProfile>;
  connectionId: string | null;
  setConnectionId: (id: string) => void;
  clusterError: string | null;
  schema: JSONSchema7;
  addConnectionProfile: (c: ConnectionProfile) => void;
}) => {
  const { isOpen, onOpen, onClose } = useDisclosure();
  const [error, setError] = useState<string | null>(null);

  const editor = (
    <JsonForm
      schema={schema}
      hasName={true}
      error={error}
      onSubmit={async d => {
        setError(null);
        const { data: connectionProfile, error } = await post('/v1/connection_profiles', {
          body: {
            name: d.name,
            connector: connector,
            config: JSON.stringify(d),
          },
        });

        if (connectionProfile) {
          addConnectionProfile(connectionProfile);
        }

        if (error) {
          setError(formatError(error));
        }

        onClose();
      }}
    />
  );

  return (
    <>
      <Stack borderWidth="1px" borderColor={'gray.500'} padding={4} borderRadius={10} maxW={800}>
        <Heading size={'xx-small'}>Cluster config</Heading>

        <Text fontSize={'sm'}>Select an existing {connector} cluster or create a new one</Text>

        <FormControl>
          <Select
            placeholder="Choose cluster"
            value={connectionId || undefined}
            onChange={c => setConnectionId(c.target.value)}
            isRequired={true}
            isInvalid={clusterError != null}
          >
            {connections
              .filter(c => c.connector == connector)
              .map(c => (
                <option key={c.id} value={c.id}>
                  {c.name} {c.description != '' && `— ${c.description}`}
                </option>
              ))}
          </Select>
          {clusterError && <FormErrorMessage>{clusterError}</FormErrorMessage>}
        </FormControl>
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
  connector,
  onSubmit,
  state,
  setState,
}: {
  connector: Connector;
  onSubmit: () => void;
  state: CreateConnectionState;
  setState: (s: CreateConnectionState) => void;
}) => {
  let { connectionProfiles, connectionProfilesLoading, mutateConnectionProfiles } =
    useConnectionProfiles();
  const [clusterError, setClusterError] = useState<string | null>(null);

  if (connectionProfilesLoading) {
    return <></>;
  }

  return (
    <Stack spacing={8}>
      {connector.connectionConfig && (
        <ClusterEditor
          connectionId={state.connectionProfileId}
          setConnectionId={c => setState({ ...state, connectionProfileId: c })}
          connector={connector.id}
          connections={connectionProfiles!}
          clusterError={clusterError}
          schema={JSON.parse(connector.connectionConfig)}
          addConnectionProfile={c => {
            mutateConnectionProfiles();
            setState({ ...state, connectionProfileId: c.id });
          }}
        />
      )}

      <Stack spacing="4" maxW={800}>
        <JsonForm
          schema={JSON.parse(connector.tableConfig)}
          initial={state.table || {}}
          onSubmit={async table => {
            if (connector?.connectionConfig && state.connectionProfileId == null) {
              setClusterError('Cluster is required');
              return;
            }
            setState({ ...state, table: table });
            onSubmit();
          }}
          error={null}
          button={'Next'}
        />
      </Stack>
    </Stack>
  );
};
