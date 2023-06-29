import { components } from '../../gen/api-types';
import React, { useState } from 'react';
import { RadioCard, RadioCardGroup } from '../../lib/RadioGroup';
import {
  Box,
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  Center,
  Divider,
  Flex,
  FormControl,
  FormHelperText,
  FormLabel,
  Heading,
  Icon,
  Input,
  Link,
  Stack,
  Switch,
  Text,
} from '@chakra-ui/react';
import { FaGlobeAmericas } from 'react-icons/fa';
import { setNestedField } from '../../lib/util';
import { SiApachekafka } from 'react-icons/si';

export interface NewConnectionProps {}

type PostConnections = components['schemas']['PostConnections'];
type HttpConnection = components['schemas']['HttpConnection'];
type KafkaConnection = components['schemas']['KafkaConnection'];

const NewConnection: React.FC<NewConnectionProps> = ({}) => {
  const [selectedConnectionType, setSelectedConnectionType] = useState<string>('Kafka');
  const [name, setName] = useState<string>('');
  const [httpConnectionState, setHttpConnectionState] = useState<HttpConnection>({
    url: '',
    headers: '',
  });
  const [authEnabled, setAuthEnabled] = useState<boolean>(false);
  const [kafkaConnectionState, setKafkaConnectionState] = useState<KafkaConnection>({
    bootstrapServers: '',
    authConfig: {
      saslAuth: {
        mechanism: '',
        password: '',
        protocol: '',
        username: '',
      },
    },
  });

  type ConnectionType = {
    name: string;
    icon: any;
    description: string;
    config: any;
    form: React.ReactElement;
  };

  const httpConnectionForm = (
    <Stack spacing={5}>
      <FormControl isRequired>
        <FormLabel>Url</FormLabel>
        <Input
          type={'text'}
          value={httpConnectionState.url}
          onChange={e =>
            setHttpConnectionState(setNestedField(httpConnectionState, 'url', e.target.value))
          }
        />
        <FormHelperText>The URL of the HTTP server to connect to</FormHelperText>
      </FormControl>
      <FormControl>
        <FormLabel>Headers</FormLabel>
        <Input
          type={'text'}
          value={httpConnectionState.headers ?? ''}
          onChange={e =>
            setHttpConnectionState(setNestedField(httpConnectionState, 'headers', e.target.value))
          }
        />
        <FormHelperText>Comma-seperated list of headers to send</FormHelperText>
      </FormControl>
    </Stack>
  );

  const kafkaAuthConfigForm = (
    <Stack spacing={5}>
      <FormControl isRequired>
        <FormLabel>Protocol</FormLabel>
        <Input
          type={'text'}
          value={kafkaConnectionState.authConfig.saslAuth?.protocol}
          onChange={e => {
            setKafkaConnectionState(
              setNestedField(kafkaConnectionState, 'authConfig.saslAuth.protocol', e.target.value)
            );
          }}
        />
        <FormHelperText>The SASL protocol used. SASL_PLAINTEXT, SASL_SSL, etc.</FormHelperText>
      </FormControl>
      <FormControl isRequired>
        <FormLabel>Mechanism</FormLabel>
        <Input
          type={'text'}
          value={kafkaConnectionState.authConfig.saslAuth?.mechanism}
          onChange={e => {
            setKafkaConnectionState(
              setNestedField(kafkaConnectionState, 'authConfig.saslAuth.mechanism', e.target.value)
            );
          }}
        />
        <FormHelperText>
          The SASL mechanism used for authentication (e.g., SCRAM-SHA-256, SCRAM-SHA-512 etc.)
        </FormHelperText>
      </FormControl>
      <FormControl isRequired>
        <FormLabel>Username</FormLabel>
        <Input
          type={'text'}
          value={kafkaConnectionState.authConfig.saslAuth?.username}
          onChange={e => {
            setKafkaConnectionState(
              setNestedField(kafkaConnectionState, 'authConfig.saslAuth.username', e.target.value)
            );
          }}
        />
      </FormControl>
      <FormControl isRequired>
        <FormLabel>Password</FormLabel>
        <Input
          type={'password'}
          value={kafkaConnectionState.authConfig.saslAuth?.password}
          onChange={e => {
            setKafkaConnectionState(
              setNestedField(kafkaConnectionState, 'authConfig.saslAuth.password', e.target.value)
            );
          }}
        />
      </FormControl>
    </Stack>
  );

  const kafkaConnectionForm = (
    <Stack spacing={5}>
      <FormControl isRequired>
        <FormLabel>Bootstrap Servers</FormLabel>
        <Input
          type={'text'}
          value={kafkaConnectionState.bootstrapServers}
          onChange={e =>
            setKafkaConnectionState({ ...kafkaConnectionState, bootstrapServers: e.target.value })
          }
        />
        <FormHelperText>Comma-separated list of kafka brokers to connect to</FormHelperText>
      </FormControl>
      <FormControl>
        <FormLabel>SASL Authentication</FormLabel>
        <Switch isChecked={authEnabled} onChange={_ => setAuthEnabled(!authEnabled)} />
      </FormControl>
      {authEnabled && kafkaAuthConfigForm}
    </Stack>
  );

  const connectionTypes: ConnectionType[] = [
    {
      name: 'Kafka',
      icon: SiApachekafka,
      description: 'Confluent Cloud, Amazon MSK, or self-hosted',
      config: { kafka: kafkaConnectionState },
      form: kafkaConnectionForm,
    },
    {
      name: 'HTTP',
      icon: FaGlobeAmericas,
      description: 'HTTP/HTTPS server',
      config: { http: httpConnectionState },
      form: httpConnectionForm,
    },
  ];

  const radioCards = connectionTypes.map(t => (
    <RadioCard key={t.name} value={t.name}>
      <Stack direction="row" spacing={5}>
        <Center>
          <Icon as={t.icon} boxSize="7" />
        </Center>
        <Box>
          <Text color="emphasized" fontWeight="medium" fontSize="sm" textTransform="capitalize">
            {t.name}
          </Text>
          <Text color="muted" fontSize="sm">
            {t.description}
          </Text>
        </Box>
      </Stack>
    </RadioCard>
  ));

  const postConnections: PostConnections = {
    name,
    config: connectionTypes.find(c => c.name == selectedConnectionType)!.config,
  };

  const handleConnectionTypeChange = (t: string) => {
    setSelectedConnectionType(t);
  };
  const connectionForm = connectionTypes.find(c => c.name == selectedConnectionType)!.form;

  const connectionTypeSelectorContainer = (
    <Flex flexDirection={'column'}>
      <Heading size="xs">Select Connection Type</Heading>
      <RadioCardGroup
        marginTop={10}
        spacing="3"
        onChange={handleConnectionTypeChange}
        value={selectedConnectionType}
      >
        {radioCards}
      </RadioCardGroup>
    </Flex>
  );

  const form = (
    <Stack flex={1} spacing={5}>
      <FormControl isRequired>
        <Heading size="xs" marginBottom={10}>
          {`Configure ${selectedConnectionType}`}
        </Heading>
        <FormLabel>Name</FormLabel>
        <Input
          type={'text'}
          value={postConnections.name}
          onChange={e => {
            setName(e.target.value);
          }}
        />
        <FormHelperText>A unique name to identify this connection</FormHelperText>
      </FormControl>
      {connectionForm}
    </Stack>
  );

  const breadcrumb = (
    <Breadcrumb>
      <BreadcrumbItem>
        <BreadcrumbLink as={Link} href="/connections">
          Connections
        </BreadcrumbLink>
      </BreadcrumbItem>
      <BreadcrumbItem isCurrentPage>
        <BreadcrumbLink>Create Connection</BreadcrumbLink>
      </BreadcrumbItem>
    </Breadcrumb>
  );

  return (
    <Flex flexDirection={'column'} height={'100vh'} padding={5} gap={5}>
      {breadcrumb}
      <Flex justifyContent={'space-between'} gap={8} alignItems={'stretch'} flex={1}>
        {connectionTypeSelectorContainer}
        <Divider orientation={'vertical'} />
        {form}
        {/* <>{JSON.stringify(postConnections)}</> */}
      </Flex>
    </Flex>
  );
};

export default NewConnection;
