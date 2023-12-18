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
import {
  AddIcon,
  ArrowBackIcon,
  ArrowRightIcon,
  DeleteIcon,
  InfoOutlineIcon,
} from '@chakra-ui/icons';
import { ConnectionProfile, Connector, del, useConnectionProfiles } from '../../lib/data_fetching';
import { CreateConnectionState } from './CreateConnection';
import { formatError } from '../../lib/util';
import { CreateProfile } from './CreateProfile';
import Loading from '../../components/Loading';

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
  setCreatingCluster,
}: {
  connector: string;
  connections: Array<ConnectionProfile>;
  onSubmit: (c: string) => void;
  schema: JSONSchema7;
  setCreatingCluster: (b: boolean) => void;
}) => {
  const [viewingProfile, setViewingProfile] = useState<ConnectionProfile | null>(null);
  const { isOpen, onOpen, onClose } = useDisclosure();
  const toast = useToast();
  const { mutateConnectionProfiles } = useConnectionProfiles();
  const [selected, setSelected] = useState<string | null>(null);

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
      if (profile.id == selected) {
        setSelected(null);
      }
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
          Select an existing {connector}{' '}
          <dfn
            title={
              'A connection profile allows you to define common connection details once and use it across multiple connection tables'
            }
          >
            connection profile
          </dfn>{' '}
          or create a new one
        </Text>

        <Stack spacing={4} w={800}>
          {connections
            .filter(c => c.connector == connector)
            .map(c => (
              <HStack key={c.id} w={768}>
                <LinkBox
                  borderRadius={8}
                  w={'100%'}
                  p={4}
                  bg={c.id == selected ? 'blue.700' : 'gray.800'}
                  border={'1px solid #777'}
                  _hover={{ bg: c.id == selected ? 'blue.700' : 'gray.600', borderColor: 'black' }}
                >
                  <LinkOverlay href={'#'} onClick={() => setSelected(c.id)} />
                  <HStack spacing={4} overflowX={'hidden'} maxW={'620'}>
                    <Heading size={'14px'}>{c.name}</Heading>
                    <Text
                      overflowX={'hidden'}
                      textOverflow={'ellipsis'}
                      whiteSpace={'nowrap'}
                      fontStyle={'italic'}
                    >
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
                        setViewingProfile(c);
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
          <Button onClick={() => setCreatingCluster(true)}>
            <AddIcon w={3} h={3} mr={2} />
            Create new
          </Button>

          <Spacer />

          <Button
            isDisabled={selected == null}
            onClick={() => onSubmit(selected!)}
            colorScheme={'blue'}
          >
            Continue
            <ArrowRightIcon w={3} h={3} ml={2} />
          </Button>
        </HStack>
      </Stack>

      {viewingProfile && (
        <ClusterViewerModal
          isOpen={isOpen}
          onClose={onClose}
          profile={viewingProfile}
          connector={connector}
          schema={schema}
        />
      )}
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
    return <Loading />;
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
      <ClusterChooser
        onSubmit={c => {
          setState({ ...state, connectionProfileId: c });
          onSubmit();
        }}
        connector={connector.id}
        connections={connectionProfiles!}
        schema={JSON.parse(connector.connectionConfig!)}
        setCreatingCluster={setCreatingCluster}
      />
    );
  }
};
