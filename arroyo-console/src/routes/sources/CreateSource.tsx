import { ConnectError, PromiseClient } from "@bufbuild/connect-web";
import {
  Alert,
  AlertDescription,
  AlertIcon,
  Box,
  BoxProps,
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  Button,
  Center,
  Circle,
  Container,
  Divider,
  Flex,
  FormControl,
  FormHelperText,
  FormLabel,
  Heading,
  HStack,
  Icon,
  Input,
  Spacer,
  SquareProps,
  Stack,
  Text,
  useBreakpointValue,
} from "@chakra-ui/react";
import {
  ChangeEvent,
  Dispatch,
  SetStateAction,
  useCallback,
  useEffect,
  useMemo,
  useState,
} from "react";
import { HiCheck } from "react-icons/hi";

import { Link, redirect, useNavigate } from "react-router-dom";
import { ApiGrpc } from "../../gen/api_connectweb";
import { RadioCard, RadioCardGroup } from "../../lib/RadioGroup";

import { SiApachekafka } from "react-icons/si";
import { FaStream, FaSync } from "react-icons/fa";
import { RiAuctionLine } from "react-icons/ri";
import { GiMetronome } from "react-icons/gi";
import {
  Connection,
  CreateJobReq,
  CreateSourceReq,
  EventSourceSourceConfig,
  GetConnectionsReq,
  ImpulseSourceConfig,
  KafkaSourceConfig,
  NexmarkSourceConfig,
} from "../../gen/api_pb";
import { ConfigureSource } from "./ConfigureSource";
import { ChooseSchemaType, DefineSchema } from "./DefineSchema";
import { connect } from "http2";
import { TestSource } from "./TestSource";
import { ApiClient } from "../../main";

interface Helpers {
  goToNextStep: () => void;
  goToPrevStep: () => void;
  reset: () => void;
  canGoToNextStep: boolean;
  canGoToPrevStep: boolean;
  setStep: Dispatch<SetStateAction<number>>;
}

interface UseStepProps {
  maxStep: number;
  initialStep?: number;
}

export const useStep = (props: UseStepProps): [number, Helpers] => {
  const { maxStep, initialStep = 0 } = props;
  const [currentStep, setCurrentStep] = useState(initialStep);
  const canGoToNextStep = useMemo(() => currentStep + 1 <= maxStep, [currentStep, maxStep]);
  const canGoToPrevStep = useMemo(() => currentStep - 1 >= 0, [currentStep]);

  const setStep = useCallback(
    (step: unknown) => {
      const newStep = step instanceof Function ? step(currentStep) : step;
      if (newStep >= 0 && newStep <= maxStep) {
        setCurrentStep(newStep);
        return;
      }
      throw new Error("Step not valid");
    },
    [maxStep, currentStep]
  );

  const goToNextStep = useCallback(() => {
    if (canGoToNextStep) {
      setCurrentStep(step => step + 1);
    }
  }, [canGoToNextStep]);

  const goToPrevStep = useCallback(() => {
    if (canGoToPrevStep) {
      setCurrentStep(step => step - 1);
    }
  }, [canGoToPrevStep]);

  const reset = useCallback(() => {
    setCurrentStep(0);
  }, []);

  return [
    currentStep,
    {
      goToNextStep,
      goToPrevStep,
      canGoToNextStep,
      canGoToPrevStep,
      setStep,
      reset,
    },
  ];
};

interface RadioCircleProps extends SquareProps {
  isCompleted: boolean;
  isActive: boolean;
}

export const StepCircle = (props: RadioCircleProps) => {
  const { isCompleted, isActive } = props;
  return (
    <Circle
      size="5"
      marginTop={0.5}
      bg={isCompleted ? "accent" : "inherit"}
      borderWidth={isCompleted ? "0" : "2px"}
      borderColor={isActive ? "accent" : "inherit"}
    >
      {isCompleted ? (
        <Icon as={HiCheck} color="inverted" boxSize="5" />
      ) : (
        <Circle bg={isActive ? "accent" : "border"} size="3" />
      )}
    </Circle>
  );
};

interface StepProps extends BoxProps {
  title: string;
  description: string;
  isCompleted: boolean;
  isActive: boolean;
  isLastStep: boolean;
  isFirstStep: boolean;
}

export const Step = (props: StepProps) => {
  const { isActive, isCompleted, isLastStep, isFirstStep, title, description, ...stackProps } =
    props;
  return (
    <Stack spacing="4" direction="row" {...stackProps}>
      <Stack spacing="0" align="center">
        <StepCircle isActive={isActive} isCompleted={isCompleted} />
        <Divider
          orientation="vertical"
          borderWidth="1px"
          borderColor={isCompleted ? "accent" : isLastStep ? "transparent" : "inherit"}
        />
      </Stack>
      <Stack spacing="0.5" pb={isLastStep ? "0" : "8"}>
        <Text color="emphasized" fontWeight="medium">
          {title}
        </Text>
        <Text color="muted">{description}</Text>
      </Stack>
    </Stack>
  );
};

type StepDef = {
  title: string;
  description: string;
  view?: (
    state: CreateSourceReq,
    setState: Dispatch<CreateSourceReq>,
    next: (step?: number) => void,
    client: ApiClient,
    connections: Array<Connection>
  ) => JSX.Element;
};

export const steps: Array<StepDef> = [
  {
    title: "Step 1",
    description: "Select type",
    view: (state, setState, next) => (
      <CreateTypeStep state={state} setState={setState} next={next} />
    ),
  },
  {
    title: "Step 2",
    description: "Configure source",
    view: (state, setState, next, client, connections) => (
      <ConfigureSource client={client} state={state} setState={setState} next={next} connections={connections} />
    ),
  },
  {
    title: "Step 3",
    description: "Schema type",
    view: (state, setState, next) => (
      <ChooseSchemaType state={state} setState={setState} next={next} />
    ),
  },
  {
    title: "Step 4",
    description: "Define schema",
    view: (state, setState, next, client) => (
      <DefineSchema state={state} setState={setState} next={next} client={client} />
    ),
  },
  {
    title: "Step 5",
    description: "Test",
    view: (state, setState, next, client) => (
      <TestSource state={state} setState={setState} next={next} client={client} />
    ),
  },
  {
    title: "Step 6",
    description: "Publish",
    view: (state, setState, _next, client) => (
      <PublishStep state={state} setState={setState} client={client} />
    ),
  },
];

export function CreateSource({ client }: { client: ApiClient }) {
  const [state, setState] = useState<CreateSourceReq>(new CreateSourceReq({}));
  const [connections, setConnections] = useState<Array<Connection> | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      const resp = await (await client()).getConnections(new GetConnectionsReq({}));

      setConnections(resp.connections);
    };

    fetchData();
  }, []);

  const [currentStep, { setStep }] = useStep({ maxStep: steps.length, initialStep: 0 });

  const next = (step?: number) => {
    setStep(step || currentStep + 1);
  };

  if (connections != null) {
    return (
      <Container py="8" flex="1">
        <Stack spacing={{ base: "8", lg: "6" }}>
          <Breadcrumb>
            <BreadcrumbItem>
              <BreadcrumbLink as={Link} to="/sources">
                Sources
              </BreadcrumbLink>
            </BreadcrumbItem>
            <BreadcrumbItem isCurrentPage>
              <BreadcrumbLink>Create Source</BreadcrumbLink>
            </BreadcrumbItem>
          </Breadcrumb>

          <Flex direction="row">
            <Box flex="1">
              {steps[currentStep].view!(state, setState, next, client, connections)}
            </Box>
            <Box marginTop={20} marginLeft={20}>
              {steps.map((step, id) => (
                <Step
                  key={id}
                  cursor="pointer"
                  onClick={() => {
                    if (id < currentStep) {
                      setStep(id);
                    }
                  }}
                  title={step.title}
                  description={step.description}
                  isActive={currentStep === id}
                  isCompleted={currentStep > id}
                  isFirstStep={id === 0}
                  isLastStep={steps.length === id + 1}
                />
              ))}
            </Box>
          </Flex>
        </Stack>
      </Container>
    );
  } else {
    return <Container py="8" flex="1"></Container>;
  }
}

const sourceTypes = [
  {
    name: "kafka",
    icon: SiApachekafka,
    description: "Confluent Cloud, EKS, or self-hosted Kafka",
    initialState: new KafkaSourceConfig({}),
  },
  {
    name: "kinesis",
    icon: FaStream,
    description: "AWS Kinesis stream (coming soon)",
    disabled: true,
  },
  {
    name: "eventSource",
    icon: FaSync,
    description: "(also known as Server-Sent Events)",
    initialState: new EventSourceSourceConfig({}),
  },
  {
    name: "nexmark",
    icon: RiAuctionLine,
    description: "Demo source for an auction website",
    initialState: new NexmarkSourceConfig({
      eventsPerSecond: 100,
    }),
  },
  {
    name: "impulse",
    icon: GiMetronome,
    description: "Periodic demo source",
    initialState: new ImpulseSourceConfig({
      eventsPerSecond: 100,
    }),
  },
];

function CreateTypeStep({
  state,
  setState,
  next,
}: {
  state: CreateSourceReq;
  setState: Dispatch<CreateSourceReq>;
  next: (step?: number) => void;
}) {
  const handleChange = (v: string) => {
    setState({
      ...state,
      /* @ts-ignore */
      typeOneof: { case: v, value: sourceTypes.find(t => t.name == v)?.initialState },
    });
  };

  return (
    <Stack spacing="10" maxWidth={400}>
      <Heading size="xs">Select Source Type</Heading>
      <Box marginLeft={0} paddingLeft={0}>
        <Stack spacing={10}>
          <RadioCardGroup
            defaultValue="one"
            spacing="3"
            onChange={handleChange}
            value={state.typeOneof.case || undefined}
          >
            {sourceTypes.map(t => (
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
          <Button
            variant="primary"
            isDisabled={state.typeOneof.case == undefined}
            onClick={() => next()}
          >
            Continue
          </Button>
        </Stack>
      </Box>
    </Stack>
  );
}

function PublishStep({
  state,
  setState,
  client,
}: {
  state: CreateSourceReq;
  setState: Dispatch<CreateSourceReq>;
  client: ApiClient;
}) {
  const [error, setError] = useState<string | null>(null);
  const navigate = useNavigate();
  const isValid = /^[a-zA-Z_][a-zA-Z0-9_]{0,62}$/.test(state.name);

  const submit = async () => {
    try {
      let resp = await (await client()).createSource(state);
      navigate("/sources");
    } catch (e) {
      if (e instanceof ConnectError){
        setError(e.rawMessage);
      } else {
        setError("Something went wrong: " + e);
      }
    }
  };

  return (
    <Stack spacing={5} maxWidth={600}>
      <Heading size="sm">Publish Source</Heading>
      <FormControl>
        <FormLabel>Source Name</FormLabel>
        <Input
          isInvalid={state.name != "" && !isValid}
          type="text"
          value={state.name}
          onChange={v => setState(new CreateSourceReq({ ...state, name: v.target.value }))}
        />
        <FormHelperText>
          The source will be queryable in SQL using this name; it must be a {"\u00A0"}
          <dfn title="Names must start with a letter or _, contain only letters\n, numbers, and _s, and have fewer than 63 characters">
            valid SQL table name
          </dfn>
        </FormHelperText>
      </FormControl>
      {error ? (
        <Alert status="error">
          <AlertIcon />
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}
      <Button variant="primary" isDisabled={state.name == "" || !isValid} onClick={submit}>
        Publish
      </Button>
    </Stack>
  );
}
