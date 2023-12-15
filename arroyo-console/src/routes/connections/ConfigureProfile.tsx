import { JSONSchema7 } from 'json-schema';
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
  Spacer,
  Stack,
  Text,
  useDisclosure,
  IconButton,
  Box,
  LinkBox,
  LinkOverlay,
  useToast,
} from '@chakra-ui/react';
import { useState } from 'react';
import { JsonForm } from './JsonForm';
import { AddIcon, ArrowBackIcon, DeleteIcon, InfoOutlineIcon } from '@chakra-ui/icons';
import { ConnectionProfile, Connector, del, useConnectionProfiles } from '../../lib/data_fetching';
import { CreateConnectionState } from './CreateConnection';
import { formatError } from '../../lib/util';
import { CreateProfile } from './CreateProfile';

function ClusterViewerModal({
  isOpen,
  onClose,
  profile,
  connector,
  schema,
}: {
  isOpen: boolean;
  onClose: () => void;
  profile: ConnectionProfile;
  connector: string;
  schema: JSONSchema7;
}) {
  return (
    <Modal isOpen={isOpen} onClose={onClose} size={'2xl'}>
      <ModalOverlay />
      <ModalContent>
        <ModalHeader>Viewing {connector} connection</ModalHeader>
        <ModalCloseButton />
        <ModalBody p={4} px={7}>
          <JsonForm
            schema={schema}
            hasName={true}
            error={null}
            initial={profile.config}
            button={'Close'}
            buttonColor={'gray'}
            readonly={true}
            onSubmit={async () => onClose()}
          />
        </ModalBody>
      </ModalContent>
    </Modal>
  );
}

const ClusterChooser = ({
  connector,
  connections,
  onSubmit,
  schema,
}: {
  connector: string;
  connections: Array<ConnectionProfile>;
  onSubmit: (c: string) => void;
  schema: JSONSchema7;
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
                <LinkBox
                  borderRadius={8}
                  w={'100%'}
                  p={4}
                  border={'1px solid #777'}
                  _hover={{ bg: 'gray.600', borderColor: 'black' }}
                >
                  <LinkOverlay href={'#'} onClick={() => onSubmit(c.id)} />
                  <HStack spacing={4}>
                    <Heading size={'14px'}>{c.name}</Heading>
                    <Text fontStyle={'italic'}>{c.description}</Text>
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
      </Stack>

      <ClusterViewerModal
        isOpen={isOpen}
        onClose={onClose}
        profile={editingProfile}
        connector={connector}
        schema={schema}
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
  const [creatingCluster, setCreatingCluster] = useState<boolean>(false);

  if (connectionProfilesLoading) {
    return <></>;
  }

  if (creatingCluster) {
    return (
      <Stack spacing={8}>
        <Box>
          <Button leftIcon={<ArrowBackIcon />} onClick={() => setCreatingCluster(false)}>
            Select existing
          </Button>
        </Box>
        <CreateProfile
          connector={connector}
          addConnectionProfile={c => {
            mutateConnectionProfiles();
          }}
          next={id => {
            setState({ ...state, connectionProfileId: id });
            onSubmit();
          }}
        />
      </Stack>
    );
  } else {
    return (
      <Stack spacing={4}>
        <ClusterChooser
          onSubmit={c => {
            setState({ ...state, connectionProfileId: c });
            onSubmit();
          }}
          connector={connector.id}
          connections={connectionProfiles!}
          schema={JSON.parse(connector.connectionConfig!)}
        />

        <HStack>
          <Spacer />
          <Button onClick={() => setCreatingCluster(true)}>
            <AddIcon w={3} h={3} mr={2} />
            Create new
          </Button>
        </HStack>
      </Stack>
    );
  }
};
