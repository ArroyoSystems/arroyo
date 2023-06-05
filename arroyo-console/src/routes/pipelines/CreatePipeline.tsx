import { ConnectError } from '@bufbuild/connect-web';
import {
  Alert,
  AlertDescription,
  AlertIcon,
  Badge,
  Box,
  Button,
  Flex,
  HStack,
  Spacer,
  Spinner,
  Stack,
  Tab,
  TabList,
  TabPanel,
  TabPanels,
  Tabs,
  Text,
  useDisclosure,
} from '@chakra-ui/react';
import React, { useEffect, useMemo, useState } from 'react';
import { Link, useLocation, useNavigate } from 'react-router-dom';
import {
  BuiltinSink,
  CreatePipelineReq,
  CreateSqlJob,
  CreateUdf,
  GetPipelineReq,
  GrpcOutputSubscription,
  OutputData,
  PipelineGraphReq,
  StopType,
  UdfLanguage,
} from '../../gen/api_pb';
import { ApiClient } from '../../main';
import { Catalog } from './Catalog';
import { PipelineGraph } from './JobGraph';
import { PipelineOutputs } from './JobOutputs';
import { CodeEditor } from './SqlEditor';
import { SqlOptions } from '../../lib/types';
import {
  useJob,
  useOperatorErrors,
  usePipelineGraph,
  useSinks,
  useSources,
} from '../../lib/data_fetching';
import Loading from '../../components/Loading';
import OperatorErrors from '../../components/OperatorErrors';
import StartPipelineModal from '../../components/StartPipelineModal';

function useQuery() {
  const { search } = useLocation();

  return useMemo(() => new URLSearchParams(search), [search]);
}

export function CreatePipeline({ client }: { client: ApiClient }) {
  const [jobId, setJobId] = useState<string | undefined>(undefined);
  const { job, updateJob, jobError } = useJob(client, jobId);
  const { operatorErrors } = useOperatorErrors(client, jobId);
  const [queryInput, setQueryInput] = useState<string>('');
  const [queryInputToCheck, setQueryInputToCheck] = useState<string>('');
  const [udfsInput, setUdfsInput] = useState<string>('');
  const [udfsInputToCheck, setUdfsInputToCheck] = useState<string>('');
  const { pipelineGraph } = usePipelineGraph(client, queryInputToCheck, udfsInputToCheck);
  const { sources, sourcesLoading } = useSources(client);
  const { sinks } = useSinks(client);
  const { isOpen, onOpen, onClose } = useDisclosure();
  const [options, setOptions] = useState<SqlOptions>({ parallelism: 4, checkpointMS: 5000 });
  const navigate = useNavigate();
  const [startError, setStartError] = useState<string | null>(null);
  const [tabIndex, setTabIndex] = useState<number>(0);
  const [outputs, setOutputs] = useState<Array<{ id: number; data: OutputData }>>([]);
  const queryParams = useQuery();

  const updateQuery = (query: string) => {
    window.localStorage.setItem('query', query);
    setQueryInput(query);
  };

  const updateUdf = (udf: string) => {
    window.localStorage.setItem('udf', udf);
    setUdfsInput(udf);
  };

  useEffect(() => {
    const copyFrom = queryParams.get('from');
    const fetch = async (copyFrom: string) => {
      const def = await (
        await client()
      ).getPipeline(
        new GetPipelineReq({
          pipelineId: copyFrom,
        })
      );

      setQueryInput(def.definition || '');
      setUdfsInput(def.udfs[0].definition || '');
      setOptions({
        ...options,
        name: def.name + '-copy',
      });
    };

    let savedQuery = window.localStorage.getItem('query');
    let savedUdfs = window.localStorage.getItem('udf');
    if (copyFrom != null) {
      fetch(copyFrom);
    } else {
      if (savedQuery != null) {
        setQueryInput(savedQuery);
      }
      if (savedUdfs != null) {
        setUdfsInput(savedUdfs);
      }
    }
  }, [queryParams]);

  // Top-level loading state
  if (sourcesLoading) {
    return <Loading />;
  }

  const check = () => {
    // Setting this state triggers the uswSWR calls
    setQueryInputToCheck(queryInput);
    setUdfsInputToCheck(udfsInput);
  };

  const pipelineIsValid = async () => {
    check();
    const res = await (
      await (
        await client
      )()
    ).graphForPipeline(
      new PipelineGraphReq({
        query: queryInput,
        udfs: [new CreateUdf({ language: UdfLanguage.Rust, definition: udfsInput })],
      })
    );
    return res.result.case == 'jobGraph';
  };

  const preview = async () => {
    setOutputs([]);

    if (!(await pipelineIsValid())) {
      return;
    }

    let resp = await (
      await client()
    ).previewPipeline(
      new CreatePipelineReq({
        //name: `preview-${new Date().getTime()}`,
        name: 'preview',
        config: {
          case: 'sql',
          value: new CreateSqlJob({
            query: queryInput,
            udfs: [new CreateUdf({ language: UdfLanguage.Rust, definition: udfsInput })],
            sink: { case: 'builtin', value: BuiltinSink.Web },
            preview: true,
          }),
        },
      })
    );

    setJobId(resp.jobId);
    setTabIndex(1);

    console.log('subscribing to output');
    let counter = 1;
    let o = [];
    for await (const res of (await client()).subscribeToOutput(
      new GrpcOutputSubscription({
        jobId: resp.jobId,
      })
    )) {
      let output = {
        id: counter++,
        data: res,
      };

      o.push(output);
      if (outputs.length > 100) {
        o.shift();
      }
      setOutputs(o);
    }

    console.log('Job finished');
  };

  const run = async () => {
    check();
    if (!(await pipelineIsValid())) {
      return;
    }
    onOpen();
  };

  const start = async () => {
    try {
      let sink = sinks[options.sink!];

      let resp = await (
        await client()
      ).startPipeline(
        new CreatePipelineReq({
          name: options.name,
          config: {
            case: 'sql',
            value: new CreateSqlJob({
              query: queryInput,
              udfs: [new CreateUdf({ language: UdfLanguage.Rust, definition: udfsInput })],
              sink: sink.value,
            }),
          },
        })
      );

      localStorage.removeItem('query');
      navigate(`/jobs/${resp.jobId}`);
    } catch (e) {
      if (e instanceof ConnectError) {
        setStartError(e.rawMessage);
      } else {
        setStartError('Something went wrong');
        console.log('Unhandled error', e);
      }
    }
  };

  const startPipelineModal = (
    <StartPipelineModal
      isOpen={isOpen}
      onClose={onClose}
      startError={startError}
      options={options}
      setOptions={setOptions}
      sinks={sinks}
      start={start}
    />
  );

  let sourcesList = <></>;

  if (sources && sources.sources.length == 0) {
    sourcesList = (
      <Stack width={300} background="bg-subtle" p={2} spacing={6}>
        <Text fontSize="xl">Sources</Text>
        <Box overflowY="auto" overflowX="hidden">
          <Text>
            No sources have been configured. Create one <Link to="/sources/new">here</Link>.
          </Text>
        </Box>
      </Stack>
    );
  }

  if (sources && sources.sources.length > 0) {
    sourcesList = (
      <Stack width={300} background="bg-subtle" p={2} spacing={6}>
        <Text fontSize="xl">Sources</Text>
        <Box overflowY="auto" overflowX="hidden">
          <Catalog sources={sources!.sources} />
        </Box>
      </Stack>
    );
  }

  const previewing = job?.jobStatus?.runningDesired && job?.jobStatus?.state != 'Failed';

  let startPreviewButton = <></>;
  let stopPreviewButton = <></>;

  if (previewing) {
    stopPreviewButton = (
      <Button
        onClick={() => updateJob({ stop: StopType.Immediate })}
        size="sm"
        colorScheme="blue"
        title="Stop a preview pipeline"
        borderRadius={2}
      >
        Stop Preview
      </Button>
    );
  } else {
    startPreviewButton = (
      <Button
        onClick={preview}
        size="sm"
        colorScheme="blue"
        title="Run a preview pipeline"
        borderRadius={2}
      >
        Start Preview
      </Button>
    );
  }

  const checkButton = (
    <Button
      size="sm"
      colorScheme="blue"
      onClick={() => {
        check();
        setTabIndex(0);
      }}
      title="Check that the SQL is valid"
      borderRadius={2}
    >
      Check
    </Button>
  );

  const startPipelineButton = (
    <Button size="sm" colorScheme="green" onClick={run} borderRadius={2}>
      Start Pipeline
    </Button>
  );

  const actionBar = (
    <HStack spacing={4} p={2} backgroundColor="gray.500">
      {checkButton}
      {startPreviewButton}
      {stopPreviewButton}
      <Spacer />
      {startPipelineButton}
    </HStack>
  );

  let previewPipelineTab = (
    <TabPanel height="100%" position="relative">
      <Text>Check your SQL to see the pipeline graph.</Text>
    </TabPanel>
  );

  if (pipelineGraph?.result.case == 'jobGraph') {
    previewPipelineTab = (
      <TabPanel height="100%" position="relative">
        <Box
          style={{
            top: 0,
            bottom: 0,
            left: 0,
            right: 0,
            position: 'absolute',
          }}
          overflow="auto"
        >
          <PipelineGraph graph={pipelineGraph.result.value} setActiveOperator={() => {}} />
        </Box>
      </TabPanel>
    );
  }

  let previewResultsTabContent = <Text>Preview your SQL to see outputs.</Text>;

  if (outputs.length) {
    previewResultsTabContent = (
      <Box
        style={{
          top: 0,
          bottom: 0,
          left: 0,
          right: 0,
          position: 'absolute',
        }}
        overflow="auto"
      >
        <PipelineOutputs outputs={outputs} />
      </Box>
    );
  } else {
    if (previewing) {
      previewResultsTabContent = <Text>Launching preview pipeline...</Text>;
    }
  }

  const previewResultsTab = (
    <TabPanel overflowX="auto" height="100%" position="relative">
      {previewResultsTabContent}
    </TabPanel>
  );

  const errorsTab = (
    <TabPanel overflowX="auto" height="100%" position="relative">
      {operatorErrors ? (
        <OperatorErrors operatorErrors={operatorErrors} />
      ) : (
        <Text>Job errors will appear here.</Text>
      )}
    </TabPanel>
  );

  const previewTabsContent = (
    <TabPanels height="calc(100% - 40px)" width={'100%'}>
      {previewPipelineTab}
      {previewResultsTab}
      {errorsTab}
    </TabPanels>
  );

  const editorTabs = (
    <Box padding={5} pl={0} backgroundColor="#1e1e1e">
      <Tabs>
        <TabList>
          <Tab>query.sql</Tab>
          <Tab>udfs.rs</Tab>
        </TabList>
        <TabPanels>
          <TabPanel>
            <CodeEditor query={queryInput} setQuery={updateQuery} />
          </TabPanel>
          <TabPanel>
            <CodeEditor query={udfsInput} setQuery={updateUdf} language="rust" />
          </TabPanel>
        </TabPanels>
      </Tabs>
    </Box>
  );

  let errorMessage;
  if (pipelineGraph?.result.case == 'errors') {
    errorMessage = pipelineGraph.result.value.errors[0].message;
  } else if (job?.jobStatus?.state == 'Failed') {
    errorMessage = 'Job failed. See "Errors" tab for more details.';
  } else if (jobError && jobError instanceof ConnectError) {
    errorMessage = jobError.rawMessage;
  } else {
    errorMessage = '';
  }

  let errorComponent = <></>;
  if (errorMessage) {
    errorComponent = (
      <Alert status="error">
        <AlertIcon />
        <AlertDescription>{errorMessage}</AlertDescription>
      </Alert>
    );
  }

  const previewTabs = (
    <Tabs index={tabIndex} onChange={i => setTabIndex(i)} height="100%">
      {
        <TabList>
          <Tab>Pipeline</Tab>
          <Tab>
            <HStack>
              <Text>Results</Text>
              {previewing ? <Spinner size="xs" speed="0.9s" /> : null}
            </HStack>
          </Tab>
          <Tab>
            <Text>Errors</Text>
            {(operatorErrors?.messages?.length || 0) > 0 && (
              <Badge ml={2} colorScheme="red" size={'xs'}>
                {operatorErrors!.messages.length}
              </Badge>
            )}
          </Tab>
        </TabList>
      }
      {previewTabsContent}
    </Tabs>
  );

  return (
    <Flex height={'100vh'}>
      <Flex>{sourcesList}</Flex>
      <Flex direction={'column'} flex={1}>
        {editorTabs}
        {actionBar}
        {errorComponent}
        {previewTabs}
      </Flex>
      {startPipelineModal}
    </Flex>
  );
}
