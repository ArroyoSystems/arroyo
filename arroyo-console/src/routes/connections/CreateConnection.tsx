import { Link, useNavigate, useParams } from 'react-router-dom';
import { ApiClient } from '../../main';
import {
  Box,
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  Button,
  Container,
  Divider,
  Flex,
  FormControl,
  FormHelperText,
  FormLabel,
  HStack,
  Heading,
  Icon,
  Input,
  Modal,
  ModalBody,
  ModalCloseButton,
  ModalContent,
  ModalHeader,
  ModalOverlay,
  Select,
  Spacer,
  Stack,
  Step,
  StepDescription,
  StepIcon,
  StepIndicator,
  StepNumber,
  StepSeparator,
  StepStatus,
  StepTitle,
  Stepper,
  Text,
  useDisclosure,
  useSteps,
} from '@chakra-ui/react';
import { useConnections, useConnectors } from '../../lib/data_fetching';
import { useEffect, useState } from 'react';
import { Connection, Connector, CreateConnectionReq } from '../../gen/api_pb';
import { AddIcon } from '@chakra-ui/icons';

import { Form as RJSForm } from '@rjsf/chakra-ui';
import {
  WidgetProps,
  RegistryWidgetsType,
  FormContextType,
  RJSFSchema,
  RegistryFieldsType,
  StrictRJSFSchema,
  TitleFieldProps,
} from '@rjsf/utils';
import validator from '@rjsf/validator-ajv8';
import MySelectWidget from './MySelectWidget';
import { JsonForm } from './JsonForm';
import { JSONSchema7 } from 'json-schema';
import { ConnectError } from '@bufbuild/connect-web';

export default function CustomTitle<
  T = any,
  S extends StrictRJSFSchema = RJSFSchema,
  F extends FormContextType = any
>({ id, title }: TitleFieldProps<T, S, F>) {
  return (
    <Box id={id} mt={1} mb={4}>
      <Heading>f{title}</Heading>
      <Divider />
    </Box>
  );
}
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
              <option key={c.id} value={c.name}>
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
}: {
  client: ApiClient;
  connector: Connector;
  onSubmit: (connectionId: string | null, table: any) => void;
}) => {
    let { connections, connectionsLoading, mutateConnections } = useConnections(client);
    let [connectionId, setConnectionId] = useState<string | null>(null);

    if (connectionsLoading) {
      return <></>;
    }


    return (
      <Stack spacing={8}>
        {connector?.connectionConfig && (
          <ClusterEditor
            client={client}
            connectionId={connectionId}
            setConnectionId={setConnectionId}
            connector={connector.id}
            connections={connections!}
            schema={JSON.parse(connector.connectionConfig)}
            addCluster={c => {
              mutateConnections([...connections!, c]);
              setConnectionId(c.id);
            }}
          />
        )}

        <Stack spacing="4" maxW={800}>
          <JsonForm
            schema={JSON.parse(connector!.tableConfig)}
            onSubmit={async (table) => {
              onSubmit(connectionId, table);
            }}
            error={null}
            button={connector!.customSchemas ? 'Next' : 'Create'}
          />
        </Stack>
      </Stack>
    );
};

export const ConnectionCreator = ({
  client,
  connector,
}: {
  client: ApiClient;
  connector: Connector;
}) => {
  const [connectionId, setConnectionId] = useState<string | null>(null);
  const [table, setTable] = useState<any>(null);


  let steps = [{ title: 'Configure cluster' }, { title: 'Define schema' }, { title: 'Test' }];

  if (!connector!.customSchemas) {
    steps.splice(1, 1);
  }

  const { activeStep, setActiveStep } = useSteps({
    index: 1,
    count: steps.length,
  });

  return (
    <Stack spacing={8}>
      <Stepper index={activeStep}>
        {steps.map((step, index) => (
          <Step key={index}>
            <StepIndicator>
              <StepStatus
                complete={<StepIcon />}
                incomplete={<StepNumber />}
                active={<StepNumber />}
              />
            </StepIndicator>

            <Box flexShrink="0">
              <StepTitle>{step.title}</StepTitle>
            </Box>

            <StepSeparator />
          </Step>
        ))}
      </Stepper>

      { activeStep === 1 &&
        <ConfigureConnection client={client} connector={connector} onSubmit={(connectionId, table) => {
          setConnectionId(connectionId);
          setTable(table);
          setActiveStep(2);
        }} /> }

    </Stack>
  );
};

export const CreateConnection = ({ client }: { client: ApiClient }) => {
  let { connectorId } = useParams();
  let { connectors, connectorsLoading } = useConnectors(client);

  let navigate = useNavigate();

  let connector = connectors?.connectors.find(c => c.id === connectorId);

  useEffect(() => {
    if (connectors != null && connector == null) {
      navigate('/connections/new');
    }
  });

  if (connectorsLoading) {
    return <></>;
  } else {
    return (
      <Container py="8" flex="1">
        <Stack spacing={8} maxW={800}>
          <Breadcrumb>
            <BreadcrumbItem>
              <BreadcrumbLink as={Link} to="/connections">
                Connections
              </BreadcrumbLink>
            </BreadcrumbItem>
            <BreadcrumbItem>
              <BreadcrumbLink as={Link} to="/connections/new">
                Create Connection
              </BreadcrumbLink>
            </BreadcrumbItem>
            <BreadcrumbItem isCurrentPage>
              <BreadcrumbLink>{connector?.name}</BreadcrumbLink>
            </BreadcrumbItem>
          </Breadcrumb>

          <Stack spacing="4">
            <Heading size="sm">Create {connector?.name} connection</Heading>
          </Stack>

          <ConnectionCreator client={client} connector={connector!} />
        </Stack>
      </Container>
    );
  }
};
