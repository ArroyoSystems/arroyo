import { PromiseClient } from "@bufbuild/connect-web";
import {
  Box,
  Button,
  ColorProps,
  Container,
  Heading,
  HStack,
  SimpleGrid,
  Stack,
  Text,
  useBreakpointValue,
  useColorModeValue,
} from "@chakra-ui/react";
import { useEffect, useState } from "react";
import { useLinkClickHandler } from "react-router-dom";
import { ApiGrpc } from "../../gen/api_connectweb";
import { GetJobsReq, GetSinksReq, GetSourcesReq, JobStatus, Sink, SourceDef } from "../../gen/api_pb";
import { ApiClient } from "../../main";

interface HomeState {
  jobs: Array<JobStatus> | null;
  sources: Array<SourceDef> | null;
  sinks: Array<Sink> | null;
}

interface Props {
  label: string;
  value?: string;
  color?: string;
}
export const Stat = (props: Props) => {
  const { label, value, color, ...boxProps } = props;
  return (
    <Box
      px={{ base: "4", md: "6" }}
      py={{ base: "5", md: "6" }}
      bg="bg-surface"
      height={120}
      borderRadius="lg"
      boxShadow={useColorModeValue("sm", "sm-dark")}
      {...boxProps}
    >
      <Stack>
        <Text fontSize="sm" color="muted">
          {label}
        </Text>
        <Heading size={useBreakpointValue({ base: "sm", md: "md" })} color={color}>{value}</Heading>
      </Stack>
    </Box>
  );
};

export function Home({ client }: { client: ApiClient }) {
  const [state, setState] = useState<HomeState>({ jobs: null, sources: null, sinks: null });

  useEffect(() => {
    const fetchData = async () => {
      const clientObj = (await client());
      const jobs = clientObj.getJobs(new GetJobsReq({}));
      const sources = clientObj.getSources(new GetSourcesReq({}));
      const sinks = clientObj.getSinks(new GetSinksReq({}));

      setState({
        jobs: (await jobs).jobs,
        sources: (await sources).sources,
        sinks: (await sinks).sinks,
      });
    };

    fetchData();
  }, []);

  let runningJobs = state.jobs?.filter(j => j.state == "Running" ||
    j.state == "Checkpointing" || j.state == "Compacting").length.toString();
  let allJobs = state.jobs?.length.toString();
  let failedJobs = state.jobs?.filter(j => j.state == "Failed").length;

  return (
    <Container py="8" flex="1">
      <Stack spacing={{ base: "8", lg: "6" }}>
        <Stack
          spacing="4"
          direction={{ base: "column", lg: "row" }}
          justify="space-between"
          align={{ base: "start", lg: "center" }}
        >
          <Stack spacing="1">
            <Heading size="sm" fontWeight="medium">
              Dashboard
            </Heading>
          </Stack>
          <HStack spacing="3">
            <Button variant="primary" onClick={useLinkClickHandler("/pipelines/new")}>
              Create Pipeline
            </Button>
          </HStack>
        </Stack>
        <Stack spacing={{ base: "5", lg: "6" }}>
          <SimpleGrid columns={{ base: 1, md: 3 }} gap="6">
            <Stat label="Running Jobs" value={runningJobs} />
            <Stat label="All Jobs" value={allJobs} />
            <Stat label="Failed Jobs" value={failedJobs?.toString()} color={ failedJobs != null && failedJobs > 0 ? "red.300" : undefined}  />
            <Stat label="Sources" value={state.sources?.length.toString()} />
            <Stat label="Sinks" value={state.sinks?.length.toString()} />
          </SimpleGrid>
        </Stack>
      </Stack>
    </Container>
  );
}
