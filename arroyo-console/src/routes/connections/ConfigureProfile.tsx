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
  Table,
  Thead,
  Tr,
  Th,
  Tbody,
  IconButton,
  SimpleGrid,
  Box, Link, Grid, Card, LinkBox, LinkOverlay, useToast,
} from '@chakra-ui/react';
import { useState } from 'react';
import { JsonForm } from './JsonForm';
import {AddIcon, ArrowForwardIcon, DeleteIcon, EditIcon, InfoIcon, InfoOutlineIcon} from '@chakra-ui/icons';
import {ConnectionProfile, Connector, del, post, useConnectionProfiles} from '../../lib/data_fetching';
import { CreateConnectionState } from './CreateConnection';
import { formatError } from '../../lib/util';

function ClusterEditorModal({
  isOpen,
  onClose,
  editingProfile,
  connector,
  schema,
  addConnectionProfile,
}: {
  isOpen: boolean;
  onClose: () => void;
  editingProfile: ConnectionProfile;
  connector: string;
  schema: JSONSchema7;
  addConnectionProfile: (c: ConnectionProfile) => void;
}) {
  const [error, setError] = useState<string | null>(null);

  return (
    <Modal isOpen={isOpen} onClose={onClose} size={'2xl'}>
      <ModalOverlay />
      <ModalContent>
        <ModalHeader>
          {editingProfile.id ? 'Update' : 'Create'} {connector} connection
        </ModalHeader>
        <ModalCloseButton />
        <ModalBody p={4} px={7}>
          <JsonForm
            schema={schema}
            hasName={true}
            error={error}
            initial={editingProfile.config || {}}
            button={editingProfile.id ? 'Close' : 'Create'}
            readonly={editingProfile.id != undefined}
            onSubmit={async d => {
              if (editingProfile.id) {
                onClose();
                return;
              }
              setError(null);
              const { data: connectionProfile, error } = await post('/v1/connection_profiles', {
                body: {
                  name: d.name,
                  connector: connector,
                  config: d,
                },
              });

              if (connectionProfile) {
                addConnectionProfile(connectionProfile);
              }

              if (error) {
                setError(formatError(error));
                return;
              }

              onClose();
            }}
          />
        </ModalBody>
      </ModalContent>
    </Modal>
  );
}

const ClusterEditor = ({
  connector,
  connections,
  onSubmit,
  clusterError,
  schema,
  addConnectionProfile,
}: {
  connector: string;
  connections: Array<ConnectionProfile>;
  onSubmit: (c: string) => void;
  clusterError: string | null;
  schema: JSONSchema7;
  addConnectionProfile: (c: ConnectionProfile) => void;
}) => {
  const [editingProfile, setEditingProfile] = useState<any>({});
  const { isOpen, onOpen, onClose } = useDisclosure();
  const toast = useToast();
  const { mutateConnectionProfiles } = useConnectionProfiles();

  const deleteProfile = async (profile: ConnectionProfile) => {
    const { error } = await del('/v1/connection_profiles/{id}', {
      params: { path: { id: profile.id } },
    });
    mutateConnectionProfiles();
    if (error) {
      toast({
        title: 'Failed to delete connection profile',
        description: formatError(error),
        status: 'error',
        duration: 9000,
        isClosable: true,
      });
    } else {
      toast({
        title: 'Connection profile deleted',
        description: `Successfully deleted connection profile ${profile.name}`,
        status: 'success',
        duration: 9000,
        isClosable: true,
      });
    }
  };


  return (
    <>
      <Stack spacing={4} padding={4} maxW={800}>
        <Heading size={'xx-small'}>Cluster config</Heading>

        <Text fontSize={'sm'}>
          Select an existing {connector} connection profile or create a new one
        </Text>

        <Stack spacing={4}>
          {connections
            .filter(c => c.connector == connector)
            .map(c => (
              <HStack key={c.id}>
                <LinkBox  borderRadius={8} w={"100%"} p={4} border={"1px solid #777"} _hover={{ bg: "gray.600", borderColor: "black" }}>
                  <LinkOverlay href={"#"} onClick={() => onSubmit(c.id)} />
                  <HStack spacing={4}>
                    <Heading size={"14px"}>{c.name}</Heading>
                    <Text fontStyle={"italic"}>
                      {c.description}
                    </Text>
                  </HStack>
                </LinkBox>
                <Spacer />
                <Box>
                  <HStack>
                    <IconButton
                      variant={'outline'}
                      aria-label={'Info'}
                      icon={<InfoOutlineIcon />}
                      onClick={() => {
                        setEditingProfile(c);
                        onOpen();
                      }}
                    />

                    <IconButton
                      variant={'outline'}
                      aria-label={'Delete'}
                      icon={<DeleteIcon />}
                      onClick={() => deleteProfile(c)}
                    />
                  </HStack>
                </Box>
              </HStack>
            ))}
        </Stack>

        <HStack>
          <Spacer />
          <Button
            onClick={() => {
              setEditingProfile({});
              onOpen();
            }}
          >
            <AddIcon w={3} h={3} mr={2} />
            Create new
          </Button>
        </HStack>
      </Stack>

      <ClusterEditorModal
        isOpen={isOpen}
        onClose={onClose}
        editingProfile={editingProfile}
        connector={connector}
        schema={schema}
        addConnectionProfile={addConnectionProfile}
      />
    </>
  );
};

export const ConfigureProfile = ({
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
    <ClusterEditor
      onSubmit={c => {
        setState({ ...state, connectionProfileId: c });
        onSubmit();
      }}
      connector={connector.id}
      connections={connectionProfiles!}
      clusterError={clusterError}
      schema={JSON.parse(connector.connectionConfig!)}
      addConnectionProfile={c => {
        mutateConnectionProfiles();
      }}
    />
  );
};
