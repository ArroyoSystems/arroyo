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
  Select,
  Spinner,
  Stack,
  Text,
  useDisclosure,
} from "@chakra-ui/react";
import { ChangeEvent, ChangeEventHandler, Dispatch, useEffect, useRef, useState } from "react";
import { FaStream } from "react-icons/fa";
import { FiDatabase } from "react-icons/fi";
import { SiApachekafka } from "react-icons/si";
import { Link, useNavigate } from "react-router-dom";
import { ApiGrpc } from "../../gen/api_connectweb";
import {
    Connection,
  CreateSinkReq,
  GetConnectionsReq,
  KafkaSinkConfig,
  TestSchemaResp,
  TestSourceMessage,
} from "../../gen/api_pb";
import { RadioCardGroup, RadioCard } from "../../lib/RadioGroup";
import { ApiClient } from "../../main";
import { Connections } from "../connections/Connections";

function onChangeString(
  state: CreateSinkReq,
  setState: Dispatch<CreateSinkReq>,
  field: string,
  config: any,
  nullable: boolean = false,
) {
  return (v: ChangeEvent<HTMLInputElement | HTMLSelectElement>) => {
    if (nullable && v.target == null) {
      config[field] = null;
    } else {
      config[field] = v.target.value;
    }
    setState(
      new CreateSinkReq({
        ...state,
        sinkType: { case: state.sinkType.case, value: config },
      })
    );
  };
}

function ConfigureKafka({
  state,
  setState,
  setReady,
  connections
}: {
  state: CreateSinkReq;
  setState: Dispatch<CreateSinkReq>;
  setReady: Dispatch<boolean>;
  connections: Array<Connection>;
}) {
  const config = state.sinkType.value as KafkaSinkConfig;

  function updateReady<T>(f: (a: T) => void) {
    return (a: T) => {
        f(a);
        setReady(config.topic != "" && config.connection != "");
    };
  }

  return (
    <Stack spacing={5}>
      <FormControl isRequired>
        <FormLabel>Kafka Connection</FormLabel>
        <Select
          placeholder="Select connection"
          value={config.connection}
          onChange={updateReady(onChangeString(state, setState, "connection", config))}
        >
          {connections
            .filter(c => c.connectionType.case == "kafka")
            .map(c => (
              <option key={c.name} value={c.name}>
                {c.name}
              </option>
            ))}
        </Select>
        <FormHelperText>
          Choose the Kafka cluster to connect to, or set up a new one{" "}
          <Link to="/connections/new">here</Link>
        </FormHelperText>
      </FormControl>

      <FormControl isRequired>
        <FormLabel>Topic</FormLabel>
        <Input
          type="text"
          value={config.topic}
          onChange={updateReady(onChangeString(state, setState, "topic", config))}
        />
        <FormHelperText>Kafka Topic that data will be written to</FormHelperText>
      </FormControl>
    </Stack>
  );
}

export function SinkEditor({
  client,
  edit,
}: {
  client: ApiClient;
  edit?: CreateSinkReq;
}) {
  const [state, setState] = useState<CreateSinkReq>(edit || new CreateSinkReq({}));
  const [connections, setConnections] = useState<Array<Connection>>([]);
  const [ready, setReady] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  const navigate = useNavigate();

  useEffect(() => {
    const fetchData = async () => {
      const resp = await (await client()).getConnections(new GetConnectionsReq({}));

      setConnections(resp.connections);
    };

    fetchData();
  }, []);


  const sinkTypes = [
    {
      name: "kafka",
      icon: SiApachekafka,
      description: "Output to a Kafka stream",
      initialState: new KafkaSinkConfig({}),
      editor: (
        <ConfigureKafka
          state={state}
          setState={setState}
          setReady={setReady}
          connections={connections}
        />
      ),
    },
    {
      name: "kinesis",
      icon: FaStream,
      description: "Output to a Kinesis stream (coming soon)",
      initialState: new KafkaSinkConfig({}),
      disabled: true,
    }
  ];

  const handleChange = (v: "kafka" | "state") => {
    setState({
      ...state,
      /* @ts-ignore */
      sinkType: { case: v, value: sinkTypes.find(t => t.name == v)?.initialState },
    });
  };

  const selectedType = sinkTypes.find(c => c.name == state.sinkType?.case);

  const onSubmit = async () => {
    try {
      await (await client()).createSink(state);
      navigate("/sinks");
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

  return (
    <Container py="8" flex="1">
      {errorAlert}
      <Stack spacing={{ base: "8", lg: "6" }}>
        <Breadcrumb>
          <BreadcrumbItem>
            <BreadcrumbLink as={Link} to="/sinks">
              Sinks
            </BreadcrumbLink>
          </BreadcrumbItem>
          <BreadcrumbItem isCurrentPage>
            <BreadcrumbLink>Create Sink</BreadcrumbLink>
          </BreadcrumbItem>
        </Breadcrumb>

        <Flex direction="row">
          <Stack spacing="4" width={400}>
            <Heading size="xs">Select Sink Type</Heading>
            <Box marginLeft={0} paddingLeft={0}>
              <Stack spacing={10}>
                <RadioCardGroup
                  defaultValue="one"
                  spacing="3"
                  onChange={handleChange}
                  value={state.sinkType.case || undefined}
                >
                  {sinkTypes.map(t => (
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
                          setState(new CreateSinkReq({ ...state, name: v.target.value }))
                        }
                      />
                      <FormHelperText>
                        A unique name to identify this sink
                      </FormHelperText>
                    </FormControl>

                    {selectedType.editor}
                    <Button
                      variant="primary"
                      disabled={!ready || state.name == ""}
                      spinner={<Spinner />}
                      onClick={onSubmit}
                    >
                      Create
                    </Button>
                  </Stack>
                ) : null}

              </Stack>
            </Box>
          </Stack>
        </Flex>
      </Stack>
    </Container>
  );
}
