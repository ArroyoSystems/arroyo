import { ConnectError } from "@bufbuild/connect-web";
import {
  Stack,
  FormControl,
  FormLabel,
  Input,
  FormHelperText,
  Heading,
  Button,
  Select,
  Alert,
  AlertIcon,
} from "@chakra-ui/react";
import { ChangeEvent, Dispatch, useEffect, useState } from "react";
import { Link } from "react-router-dom";
import {
  Connection,
  CreateSourceReq,
  ImpulseSourceConfig,
  KafkaSourceConfig,
  NexmarkSourceConfig,
} from "../../gen/api_pb";
import { ApiClient } from "../../main";
import { steps } from "./CreateSource";

function ConfigureImpulse({
  state,
  setState,
}: {
  state: CreateSourceReq;
  setState: Dispatch<CreateSourceReq>;
}) {
  const config = state.typeOneof.value as ImpulseSourceConfig;

  return (
    <Stack spacing={5}>
      <FormControl isRequired>
        <FormLabel>Event rate (messages / sec)</FormLabel>
        <Input
          type="number"
          value={config.eventsPerSecond}
          onChange={onChangeNumber(state, setState, "eventsPerSecond", config)}
        />
        <FormHelperText>
          The number of messages the Impulse source will emit per second
        </FormHelperText>
      </FormControl>

      <FormControl>
        <FormLabel>Event time interval (ms)</FormLabel>
        <Input
          type="number"
          value={config.intervalMicros}
          onChange={onChangeNumber(state, setState, "intervalMs", config)}
        />
        <FormHelperText>
          The number of microseconds in between the event times of subsequent events emmitted by the
          source; if not set wall-clock time is used
        </FormHelperText>
      </FormControl>

      <FormControl>
        <FormLabel>Total messages</FormLabel>
        <Input
          type="number"
          value={config.totalMessages}
          onChange={onChangeNumber(state, setState, "totalMessages", config, true)}
        />
        <FormHelperText>
          If set, the source will finish about sending this many messages.
        </FormHelperText>
      </FormControl>
    </Stack>
  );
}

function onChangeNumber(
  state: CreateSourceReq,
  setState: Dispatch<CreateSourceReq>,
  field: string,
  config: any,
  nullable: boolean = false,
  transform: (v: number) => number = v => v
) {
  return (v: ChangeEvent<HTMLInputElement>) => {
    if (nullable && v.target.value == null) {
      config[field] = null;
    } else {
      config[field] = transform(Number(v.target.value));
    }
    setState(
      new CreateSourceReq({ ...state, typeOneof: { case: state.typeOneof.case, value: config } })
    );
  };
}

function onChangeString(
  state: CreateSourceReq,
  setState: Dispatch<CreateSourceReq>,
  field: string,
  config: any,
  nullable: boolean = false
) {
  return (v: ChangeEvent<HTMLInputElement | HTMLSelectElement>) => {
    if (nullable && v.target == null) {
      config[field] = null;
    } else {
      config[field] = v.target.value;
    }
    setState(
      new CreateSourceReq({ ...state, typeOneof: { case: state.typeOneof.case, value: config } })
    );
  };
}

function ConfigureNexmark({
  state,
  setState,
}: {
  state: CreateSourceReq;
  setState: Dispatch<CreateSourceReq>;
}) {
  const config = state.typeOneof.value as NexmarkSourceConfig;

  return (
    <Stack spacing={5}>
      <FormControl isRequired>
        <FormLabel>Event rate (messages / sec)</FormLabel>
        <Input
          type="number"
          value={Number(config.eventsPerSecond)}
          onChange={onChangeNumber(state, setState, "eventsPerSecond", config)}
        />
        <FormHelperText>
          The number of messages the Impulse source will emit per second
        </FormHelperText>
      </FormControl>

      <FormControl>
        <FormLabel>Runtime (seconds)</FormLabel>
        <Input
          type="number"
          value={config.runtimeMicros ? Number(config.runtimeMicros) / 1e6 : undefined}
          onChange={onChangeNumber(state, setState, "runtimeMicros", config, true, v => v * 1e6)}
        />
        <FormHelperText>
          If set, the source will finish after running for this many seconds
        </FormHelperText>
      </FormControl>
    </Stack>
  );
}

function ConfigureKafka({
  state,
  setState,
  setReady,
  connections,
  client,
}: {
  state: CreateSourceReq;
  setState: Dispatch<CreateSourceReq>;
  setReady: Dispatch<boolean>;
  connections: Array<Connection>;
  client: ApiClient;
}) {
  const config = state.typeOneof.value as KafkaSourceConfig;
  const [testing, setTesting] = useState<boolean>(false);
  const [message, setMessage] = useState<{message: string, type: "success" | "error"} | null>(null);
  const [tested, setTested] = useState<boolean>(false);


  useEffect(() => setReady(config.topic != "" && config.connection != ""));

  let errorAlert = null;
  if (message != null) {
    errorAlert = (
      <Alert status={message.type}>
        <AlertIcon />
        {message.message}
      </Alert>
    );
  }

  const test = async () => {
    setTesting(true);
    let testedState = state.clone();
    try {
      await (await client()).getSourceMetadata(testedState);
      setMessage({message: "Topic is valid", type: "success"});
    } catch (e) {
      if (e instanceof ConnectError) {
        setMessage({message: e.rawMessage, type: "error"});
      } else {
        setMessage({message: "Something went wrong while validating the source configuration", type: "error"});
      }
    }

    setTesting(false);
    setTested(true);
  };


  return (
    <Stack spacing={5}>
      <FormControl isRequired>
        <FormLabel>Kafka Connection</FormLabel>
        <Select
          placeholder="Select connection"
          value={config.connection}
          onChange={onChangeString(state, setState, "connection", config)}
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
          onChange={onChangeString(state, setState, "topic", config)}
        />
        <FormHelperText>The Kafka topic to read from</FormHelperText>
      </FormControl>

      <Button variant="primary" onClick={test}>Validate</Button>

      {errorAlert}
    </Stack>
  );
}

export function ConfigureSource({
  state,
  setState,
  client,
  next,
  connections,
}: {
  state: CreateSourceReq;
  setState: Dispatch<CreateSourceReq>;
  client: ApiClient,
  next: (step?: number) => void;
  connections: Array<Connection>;
}) {
  const [ready, setReady] = useState<boolean>(true);

  const forms = new Map([
    ["impulse", <ConfigureImpulse state={state} setState={setState} />],
    ["nexmark", <ConfigureNexmark state={state} setState={setState} />],
    [
      "kafka",
      <ConfigureKafka
        state={state}
        setState={setState}
        setReady={setReady}
        connections={connections}
        client={client}
      />,
    ],
  ]);

  const onClick = async () => {
    if (state.typeOneof.case == "impulse" || state.typeOneof.case == "nexmark") {
      next(steps.length - 1);
    } else {
      next();
    }
  };


  return (
    <Stack spacing={10} maxWidth={500}>
      <Heading size="xs">Configure {state.typeOneof.case} source</Heading>
      {forms.get(state.typeOneof.case!)}

      <Button variant="primary" onClick={onClick} disabled={!ready}>
        Continue
      </Button>
    </Stack>
  );
}
