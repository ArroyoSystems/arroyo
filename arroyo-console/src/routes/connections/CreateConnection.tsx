import { ConnectError, PromiseClient } from "@bufbuild/connect-web";
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
} from "@chakra-ui/react";
import { ChangeEvent, ChangeEventHandler, Dispatch, useEffect, useRef, useState } from "react";
import { FaStream } from "react-icons/fa";
import { SiApachekafka } from "react-icons/si";
import { ApiGrpc } from "../../gen/api_connectweb";
import {
  CreateConnectionReq,
  KafkaAuthConfig,
  KafkaConnection,
  KinesisConnection,
  NoAuth,
  SaslAuth,
  TestSchemaResp,
  TestSourceMessage,
} from "../../gen/api_pb";
import { RadioCardGroup, RadioCard } from "../../lib/RadioGroup";
import { ApiClient } from "../../main";

function onChangeString(
  state: CreateConnectionReq,
  setState: Dispatch<CreateConnectionReq>,
  field: string,
  config: any,
  nullable: boolean = false
) {
  return (v: ChangeEvent<HTMLInputElement>) => {
    if (nullable && v.target == null) {
      config[field] = null;
    } else {
      config[field] = v.target.value;
    }
    setState(
      new CreateConnectionReq({
        ...state,
        connectionType: { case: state.connectionType.case, value: config },
      })
    );
  };
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

  const onChange = (e: ChangeEvent<HTMLInputElement>) => {
    onChangeString(state, setState, "bootstrapServers", config)(e);
    setReady(e.target.value != "");
  };
  const onChangeAuthType = (e: ChangeEvent<HTMLSelectElement>) => {
    let authConfig;

    switch (e.target.value) {
      case "noAuth":
        authConfig = new KafkaAuthConfig({ authType: { case: "noAuth", value: new NoAuth({}) } });
        break;
      case "saslAuth":
        authConfig = new KafkaAuthConfig({ authType: { case: "saslAuth", value: new SaslAuth({}) } });
        break;
    }

    config.authConfig = authConfig;
    setState(
      new CreateConnectionReq({
        ...state,
        connectionType: { case: state.connectionType.case as "kafka" | "kinesis", value: config },
      })
    );
  };

  const onChangeSaslProtocol = (e: ChangeEvent<HTMLInputElement>) => {
    const updatedValue = { ...config.authConfig?.authType.value, protocol: e.target.value };
    config.authConfig = new KafkaAuthConfig({ authType: { case: "saslAuth", value: updatedValue } });
    setState(
      new CreateConnectionReq({
        ...state,
        connectionType: { case: state.connectionType.case as "kafka" | "kinesis", value: config },
      })
    );
  };

  const onChangeSaslField = (e: ChangeEvent<HTMLInputElement>) => {
  }

  const onChangeSaslMechanism = (e: ChangeEvent<HTMLInputElement>) => {
    const updatedValue = { ...config.authConfig?.authType.value, mechanism: e.target.value };
    config.authConfig = new KafkaAuthConfig({ authType: { case: "saslAuth", value: updatedValue } });
    setState(
      new CreateConnectionReq({
        ...state,
        connectionType: { case: state.connectionType.case as "kafka" | "kinesis", value: config },
      })
    );
  };

  const onChangeSaslUsername = (e: ChangeEvent<HTMLInputElement>) => {
    const updatedValue = { ...config.authConfig?.authType.value, username: e.target.value };
    config.authConfig = new KafkaAuthConfig({ authType: { case: "saslAuth", value: updatedValue } });
    setState(
      new CreateConnectionReq({
        ...state,
        connectionType: { case: state.connectionType.case as "kafka" | "kinesis", value: config },
      })
    );
  };

  const onChangeSaslPassword = (e: ChangeEvent<HTMLInputElement>) => {
    const updatedValue = { ...config.authConfig?.authType.value, password: e.target.value };
    config.authConfig = new KafkaAuthConfig({ authType: { case: "saslAuth", value: updatedValue } });
    setState(
      new CreateConnectionReq({
        ...state,
        connectionType: { case: state.connectionType.case as "kafka" | "kinesis", value: config },
      })
    );
  };


  return (
    <Stack spacing={5}>
      <FormControl isRequired>
        <FormLabel>Bootstrap Servers</FormLabel>
        <Input type="text" value={config.bootstrapServers} onChange={onChange} />
        <FormHelperText>Comma-separated list of kafka brokers to connect to</FormHelperText>
      </FormControl>
      <FormControl isRequired>
        <FormLabel>Authentication Type</FormLabel>
        <Select placeholder="Select authentication type" value={config.authConfig?.authType.case || ""} onChange={onChangeAuthType}>
          <option value="noAuth">No Auth</option>
          <option value="saslAuth">SASL Auth</option>
        </Select>
      </FormControl>
        {config.authConfig?.authType.case === "saslAuth" && (
          <Stack spacing={5}>
            <FormControl isRequired>
              <FormLabel>Protocol</FormLabel>
              <Input
                type="text"
                value={config.authConfig.authType.value.protocol || ""}
                onChange={onChangeSaslProtocol}
              />
              <FormHelperText>
                The SASL protocol used. SASL_PLAINTEXT, SASL_SSL, etc.
              </FormHelperText>
            </FormControl>
            <FormControl isRequired>
              <FormLabel>Mechanism</FormLabel>
              <Input
                type="text"
                value={config.authConfig.authType.value.mechanism || ""}
                onChange={onChangeSaslMechanism}
              />
              <FormHelperText>
                The SASL mechanism used for authentication (e.g., SCRAM-SHA-256, SCRAM-SHA-512 etc.)
              </FormHelperText>
            </FormControl>
            <FormControl isRequired>
              <FormLabel>Username</FormLabel>
              <Input
                type="text"
                value={config.authConfig.authType.value.username || ""}
                onChange={onChangeSaslUsername}
              />
            </FormControl>
            <FormControl isRequired>
              <FormLabel>Password</FormLabel>
              <Input
                type="password"
                value={config.authConfig.authType.value.password || ""}
                onChange={onChangeSaslPassword}
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
      name: "kafka",
      icon: SiApachekafka,
      description: "Confluent Cloud, Amazon MSK, or self-hosted",
      initialState: new KafkaConnection({}),
      editor: <ConfigureKafka state={state} setState={setState} setReady={setReady} />,
      disabled: false
    },
    {
      name: "kinesis",
      icon: FaStream,
      description: "AWS Kinesis stream (coming soon)",
      initialState: new KinesisConnection({}),
      disabled: true
    },
  ];

  const handleChange = (v: "kafka" | "kinesis") => {
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
      window.location.href = "/connections";
    } catch (e) {
      if (e instanceof ConnectError) {
        setError(e.rawMessage);
      } else {
        setError("Something went wrong... try again");
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
      <Stack spacing={{ base: "8", lg: "6" }}>
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
                      <FormHelperText>
                        A unique name to identify this Kafka connection
                      </FormHelperText>
                    </FormControl>

                    {selectedType.editor}
                    <Button
                      variant="primary"
                      disabled={!ready || state.name == "" || testing}
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
                      <Alert status={result.error ? "error" : "success"}>
                        <AlertIcon />
                        <AlertDescription>{result.message}</AlertDescription>
                      </Alert>
                    </Box>

                    <Button colorScheme={result.error ? "red" : "green"} onClick={onClickCreate}>
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
