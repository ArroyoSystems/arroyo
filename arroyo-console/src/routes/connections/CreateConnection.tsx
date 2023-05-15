import { ConnectError } from '@bufbuild/connect-web';
import {
  Alert,
  AlertDescription,
  AlertDialog,
  AlertDialogBody,
  AlertDialogContent,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogOverlay,
  AlertIcon,
  Box,
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  Button,
  Center,
  Container,
  Flex,
  FormControl,
  FormHelperText,
  FormLabel,
  Heading,
  Icon,
  Input,
  Link,
  Select,
  Spinner,
  Stack,
  Text,
  useDisclosure,
} from '@chakra-ui/react';
import { ChangeEvent, Dispatch, useRef, useState } from 'react';
import { FaGlobeAmericas, FaStream } from 'react-icons/fa';
import { SiApachekafka } from 'react-icons/si';
import {
  CreateConnectionReq,
  HttpConnection,
  KafkaAuthConfig,
  KafkaConnection,
  KinesisConnection,
  NoAuth,
  SaslAuth,
  TestSourceMessage,
} from '../../gen/api_pb';
import { RadioCardGroup, RadioCard } from '../../lib/RadioGroup';
import { ApiClient } from '../../main';

// set the value of the field in config at the dot delimited path given by field
function setField(field: string, config: any, value: any) {
  const parts = field.split('.');
  let current = config;
  for (let i = 0; i < parts.length - 1; i++) {
    current = current[parts[i]];
  }
  current[parts[parts.length - 1]] = value;
}

function onChangeString(
  state: CreateConnectionReq,
  setState: Dispatch<CreateConnectionReq>,
  field: string,
  config: any,
  nullable: boolean = false
) {
  return (v: ChangeEvent<HTMLInputElement>) => {
    if (nullable && v.target == null) {
      setField(field, config, null);
    } else {
      setField(field, config, v.target.value);
    }
    setState(
      new CreateConnectionReq({
        ...state,
        connectionType: { case: state.connectionType.case, value: config },
      })
    );
  };
}

const KafkaAuthTypes = [
  {
    name: 'None',
    case: 'noAuth',
    value: new NoAuth({}),
  },
  {
    name: 'SASL',
    case: 'saslAuth',
    value: new SaslAuth({}),
  },
];

function ConfigureHttp({
  state,
  setState,
  setReady,
}: {
  state: CreateConnectionReq;
  setState: Dispatch<CreateConnectionReq>;
  setReady: Dispatch<boolean>;
}) {
  const config = state.connectionType.value as HttpConnection;

  const onChange = (field: string) => {
    return (e: ChangeEvent<HTMLInputElement>) => {
      onChangeString(state, setState, field, config)(e);
      setReady(config.url != '');
    };
  };

  return (
    <Stack spacing={5}>
      <FormControl isRequired>
        <FormLabel>URL</FormLabel>
        <Input
          type="text"
          value={config.url}
          onChange={onChange('url')}
          placeholder="https://api.example.com"
        />
        <FormHelperText>The URL of the HTTP server to connect to</FormHelperText>
      </FormControl>
      <FormControl>
        <FormLabel>Headers</FormLabel>
        <Input
          type="text"
          value={config.headers}
          placeholder="Content-Type: application/json"
          onChange={onChangeString(state, setState, 'headers', config)}
        />
        <FormHelperText>Comma-seperated list of headers to send</FormHelperText>
      </FormControl>
    </Stack>
  );
}

function ConfigureKafka({
  state,
  setState,
  setReady,
}: {
  state: CreateConnectionReq;
  setState: Dispatch<CreateConnectionReq>;
  setReady: Dispatch<boolean>;
}) {
  const config = state.connectionType.value as KafkaConnection;

  const onChange = (field: string) => {
    return (e: ChangeEvent<HTMLInputElement>) => {
      onChangeString(state, setState, field, config)(e);
      setReady(
        config.bootstrapServers != '' &&
          (config.authConfig!.authType.case == 'noAuth' ||
            (config.authConfig?.authType.value?.mechanism != '' &&
              config.authConfig?.authType.value?.username != '' &&
              config.authConfig?.authType.value?.password != '' &&
              config.authConfig?.authType.value?.protocol != ''))
      );
    };
  };

  const onChangeAuthType = (e: ChangeEvent<HTMLSelectElement>) => {
    /* @ts-ignore */
    config.authConfig.authType = {
      /* @ts-ignore */
      case: e.target.value,
      value: KafkaAuthTypes.find(t => t.case == e.target.value)?.value,
    };
    setState(
      new CreateConnectionReq({
        ...state,
        /* @ts-ignore */
        connectionType: { case: state.connectionType.case!, value: config },
      })
    );
  };

  return (
    <Stack spacing={5}>
      <FormControl isRequired>
        <FormLabel>Bootstrap Servers</FormLabel>
        <Input
          type="text"
          value={config.bootstrapServers}
          onChange={onChange('bootstrapServers')}
        />
        <FormHelperText>Comma-separated list of kafka brokers to connect to</FormHelperText>
      </FormControl>

      <FormControl isRequired>
        <FormLabel>Authentication Type</FormLabel>
        <Select value={config.authConfig?.authType.case} onChange={onChangeAuthType}>
          {KafkaAuthTypes.map(t => (
            <option key={t.case} value={t.case}>
              {t.name}
            </option>
          ))}
        </Select>
      </FormControl>

      {config.authConfig?.authType.case === 'saslAuth' && (
        <Stack spacing={5}>
          <FormControl isRequired>
            <FormLabel>Protocol</FormLabel>
            <Input
              type="text"
              value={config.authConfig.authType.value.protocol || ''}
              onChange={onChange('authConfig.authType.value.protocol')}
            />
            <FormHelperText>The SASL protocol used. SASL_PLAINTEXT, SASL_SSL, etc.</FormHelperText>
          </FormControl>
          <FormControl isRequired>
            <FormLabel>Mechanism</FormLabel>
            <Input
              type="text"
              value={config.authConfig.authType.value.mechanism || ''}
              onChange={onChange('authConfig.authType.value.mechanism')}
            />
            <FormHelperText>
              The SASL mechanism used for authentication (e.g., SCRAM-SHA-256, SCRAM-SHA-512 etc.)
            </FormHelperText>
          </FormControl>
          <FormControl isRequired>
            <FormLabel>Username</FormLabel>
            <Input
              type="text"
              value={config.authConfig.authType.value.username || ''}
              onChange={onChange('authConfig.authType.value.username')}
            />
          </FormControl>
          <FormControl isRequired>
            <FormLabel>Password</FormLabel>
            <Input
              type="password"
              value={config.authConfig.authType.value.password || ''}
              onChange={onChange('authConfig.authType.value.password')}
            />
          </FormControl>
        </Stack>
      )}
    </Stack>
  );
}

export function ConnectionEditor({
  client,
  edit,
}: {
  client: ApiClient;
  edit?: CreateConnectionReq;
}) {
  const [state, setState] = useState<CreateConnectionReq>(edit || new CreateConnectionReq({}));
  const [ready, setReady] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [testing, setTesting] = useState<boolean>(false);
  const [result, setResult] = useState<TestSourceMessage | null>(null);
  const { isOpen, onOpen, onClose } = useDisclosure();
  const cancelRef = useRef<any>();

  const connectionTypes = [
    {
      name: 'kafka',
      icon: SiApachekafka,
      description: 'Confluent Cloud, Amazon MSK, or self-hosted',
      initialState: new KafkaConnection({
        authConfig: new KafkaAuthConfig({
          authType: { case: 'noAuth', value: new NoAuth({}) },
        }),
      }),
      editor: <ConfigureKafka state={state} setState={setState} setReady={setReady} />,
      disabled: false,
    },
    {
      name: 'kinesis',
      icon: FaStream,
      description: 'AWS Kinesis stream (coming soon)',
      initialState: new KinesisConnection({}),
      disabled: true,
    },
    {
      name: 'http',
      icon: FaGlobeAmericas,
      description: 'HTTP/HTTPS server',
      initialState: new HttpConnection({}),
      editor: <ConfigureHttp state={state} setState={setState} setReady={setReady} />,
      disabled: false,
    },
  ];

  const handleChange = (v: 'kafka' | 'kinesis' | 'http') => {
    setState({
      ...state,
      /* @ts-ignore */
      connectionType: { case: v, value: connectionTypes.find(t => t.name == v)?.initialState },
    });
  };

  const selectedType = connectionTypes.find(c => c.name == state.connectionType?.case);

  const onTest = async () => {
    setTesting(true);
    setResult(null);
    setResult(await (await client()).testConnection(state));
    setTesting(false);
  };

  const onSubmit = async () => {
    try {
      await (await client()).createConnection(state);
      window.location.href = '/connections';
    } catch (e) {
      if (e instanceof ConnectError) {
        setError(e.rawMessage);
      } else {
        setError('Something went wrong... try again');
      }
    }
  };

  let errorAlert = null;
  if (error != null) {
    errorAlert = (
      <Alert status="error">
        <AlertIcon />
        {error}
      </Alert>
    );
  }

  const onClickCreate = () => {
    if (result?.error) {
      onOpen();
    } else {
      onSubmit();
    }
  };

  return (
    <Container py="8" flex="1">
      {errorAlert}
      <Stack spacing={{ base: '8', lg: '6' }}>
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

        <Flex direction="row">
          <Stack spacing="4" width={400}>
            <Heading size="xs">Select Connection Type</Heading>
            <Box marginLeft={0} paddingLeft={0}>
              <Stack spacing={10}>
                <RadioCardGroup
                  defaultValue="one"
                  spacing="3"
                  onChange={handleChange}
                  value={state.connectionType.case || undefined}
                >
                  {connectionTypes.map(t => (
                    <RadioCard key={t.name} value={t.name} isDisabled={t.disabled}>
                      <Stack direction="row" spacing={5}>
                        <Center>
                          <Icon as={t.icon} boxSize="7" />
                        </Center>
                        <Box>
                          <Text
                            color="emphasized"
                            fontWeight="medium"
                            fontSize="sm"
                            textTransform="capitalize"
                          >
                            {t.name}
                          </Text>
                          <Text color="muted" fontSize="sm">
                            {t.description}
                          </Text>
                        </Box>
                      </Stack>
                    </RadioCard>
                  ))}
                </RadioCardGroup>
                {selectedType != null ? (
                  <Stack spacing="4" marginLeft={20}>
                    <Heading size="xs">Configure {selectedType.name}</Heading>

                    <FormControl isRequired>
                      <FormLabel>Name</FormLabel>
                      <Input
                        type="text"
                        value={state.name}
                        onChange={v =>
                          setState(new CreateConnectionReq({ ...state, name: v.target.value }))
                        }
                      />
                      <FormHelperText>A unique name to identify this connection</FormHelperText>
                    </FormControl>

                    {selectedType.editor}
                    <Button
                      variant="primary"
                      isDisabled={!ready || state.name == '' || testing}
                      isLoading={testing}
                      spinner={<Spinner />}
                      onClick={onTest}
                    >
                      Test
                    </Button>
                  </Stack>
                ) : null}

                {result != null ? (
                  <>
                    <Box bg="bg-surface">
                      <Alert status={result.error ? 'error' : 'success'}>
                        <AlertIcon />
                        <AlertDescription>{result.message}</AlertDescription>
                      </Alert>
                    </Box>

                    <Button colorScheme={result.error ? 'red' : 'green'} onClick={onClickCreate}>
                      Create
                    </Button>
                  </>
                ) : null}
              </Stack>
            </Box>
          </Stack>
        </Flex>
      </Stack>

      <AlertDialog isOpen={isOpen} leastDestructiveRef={cancelRef} onClose={onClose}>
        <AlertDialogOverlay>
          <AlertDialogContent>
            <AlertDialogHeader fontSize="lg" fontWeight="bold">
              Validation failed
            </AlertDialogHeader>

            <AlertDialogBody>
              We were not able to validate that the connection is correctly configured. You can save
              it, but may encounter issues when creating sources using it.
            </AlertDialogBody>

            <AlertDialogFooter>
              <Button ref={cancelRef} onClick={onClose}>
                Cancel
              </Button>
              <Button colorScheme="red" onClick={onSubmit} ml={3}>
                Create
              </Button>
            </AlertDialogFooter>
          </AlertDialogContent>
        </AlertDialogOverlay>
      </AlertDialog>
    </Container>
  );
}
