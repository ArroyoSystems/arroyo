import {
  Alert,
  AlertDescription,
  AlertIcon,
  Badge,
  Box,
  Button,
  Flex,
  HStack,
  Icon,
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
import { Catalog } from './Catalog';
import { PipelineGraphViewer } from './PipelineGraph';
import { CodeEditor } from './SqlEditor';
import { SqlOptions } from '../../lib/types';
import {
  OutputData,
  post,
  useConnectionTables,
  useJobOutput,
  useJobMetrics,
  useOperatorErrors,
  usePipeline,
  usePipelineGraph,
  usePipelineJobs,
  ConnectionTable,
  JobLogMessage,
} from '../../lib/data_fetching';
import Loading from '../../components/Loading';
import OperatorErrors from '../../components/OperatorErrors';
import StartPipelineModal from '../../components/StartPipelineModal';
import { formatError } from '../../lib/util';
import { WarningIcon } from '@chakra-ui/icons';
import PaginatedContent from '../../components/PaginatedContent';
import { Panel, PanelGroup, PanelResizeHandle } from 'react-resizable-panels';
import { MdDragHandle } from 'react-icons/md';
import { PipelineOutputs } from './PipelineOutputs';

function useQuery() {
  const { search } = useLocation();

  return useMemo(() => new URLSearchParams(search), [search]);
}

export function CreatePipeline() {
  const [pipelineId, setPipelineId] = useState<string | undefined>(undefined);
  const { pipeline, updatePipeline } = usePipeline(pipelineId);
  const { jobs } = usePipelineJobs(pipelineId, true);
  const job = jobs?.length ? jobs[0] : undefined;
  const { operatorErrorsPages, operatorErrorsTotalPages, setOperatorErrorsMaxPages } =
    useOperatorErrors(pipelineId, job?.id);
  const [operatorErrors, setOperatorErrors] = useState<JobLogMessage[]>([]);
  const [queryInput, setQueryInput] = useState<string>('');
  const [queryInputToCheck, setQueryInputToCheck] = useState<string>('');
  const [udfsInput, setUdfsInput] = useState<string>('');
  const [udfsInputToCheck, setUdfsInputToCheck] = useState<string>('');
  const { pipelineGraph, pipelineGraphError } = usePipelineGraph(
    queryInputToCheck,
    udfsInputToCheck
  );
  const { operatorMetricGroups } = useJobMetrics(pipelineId, job?.id);

  const { isOpen, onOpen, onClose } = useDisclosure();
  const [options, setOptions] = useState<SqlOptions>({ parallelism: 4, checkpointMS: 5000 });
  const navigate = useNavigate();
  const [startError, setStartError] = useState<string | null>(null);
  const [tabIndex, setTabIndex] = useState<number>(0);
  const [outputSource, setOutputSource] = useState<EventSource | undefined>(undefined);
  const [outputs, setOutputs] = useState<Array<{ id: number; data: OutputData }>>([]);
  const { connectionTablePages, connectionTablesLoading } = useConnectionTables(50);
  const queryParams = useQuery();
  const { pipeline: copyFrom, pipelineLoading: copyFromLoading } = usePipeline(
    queryParams.get('from') ?? undefined
  );
  const hasErrors = operatorErrorsPages?.length && operatorErrorsPages[0].data.length > 0;

  let connectionTables: ConnectionTable[] = [];
  let catalogTruncated = false;
  if (connectionTablePages?.length) {
    connectionTables = connectionTablePages[0].data;
    catalogTruncated = connectionTablePages[0].hasMore;
  }

  const updateQuery = (query: string) => {
    window.localStorage.setItem('query', query);
    setQueryInput(query);
  };

  const updateUdf = (udf: string) => {
    window.localStorage.setItem('udf', udf);
    setUdfsInput(udf);
  };

  useEffect(() => {
    let savedQuery = window.localStorage.getItem('query');
    let savedUdfs = window.localStorage.getItem('udf');
    if (copyFrom != null) {
      setQueryInput(copyFrom.query || '');
      if (copyFrom.udfs.length) {
        setUdfsInput(copyFrom.udfs[0].definition || '');
      }
      setOptions({
        ...options,
        name: copyFrom.name + '-copy',
      });
    } else {
      if (savedQuery != null) {
        setQueryInput(savedQuery);
      }
      if (savedUdfs != null) {
        setUdfsInput(savedUdfs);
      }
    }
  }, [copyFrom]);

  const sseHandler = (event: MessageEvent) => {
    const parsed = JSON.parse(event.data) as OutputData;
    const o = { id: Number(event.lastEventId), data: parsed };
    outputs.push(o);
    if (outputs.length > 20) {
      outputs.shift();
    }
    setOutputs(outputs.slice());
  };

  useEffect(() => {
    if (pipeline && job) {
      if (outputSource) {
        outputSource.close();
      }
      setOutputSource(useJobOutput(sseHandler, pipeline.id, job.id));
    }
  }, [job?.id]);

  // Top-level loading state
  if (copyFromLoading || connectionTablesLoading) {
    return <Loading />;
  }

  const check = () => {
    // Setting this state triggers the uswSWR calls
    setQueryInputToCheck(queryInput);
    setUdfsInputToCheck(udfsInput);
    setOutputs([]);
  };

  const pipelineIsValid = async () => {
    check();
    const { error } = await post('/v1/pipelines/validate', {
      body: { query: queryInput, udfs: [{ language: 'rust', definition: udfsInput }] },
    });

    return error == undefined;
  };

  const preview = async () => {
    setQueryInputToCheck('');
    setPipelineId(undefined);

    if (!(await pipelineIsValid())) {
      return;
    }

    const { data: newPipeline, error } = await post('/v1/pipelines', {
      body: {
        name: `preview-${new Date().getTime()}`,
        parallelism: 1,
        preview: true,
        query: queryInput,
        udfs: [{ language: 'rust', definition: udfsInput }],
      },
    });

    if (error) {
      console.log('Create pipeline failed');
    }

    // Setting the pipeline id will trigger fetching the job and subscribing to the output
    setPipelineId(newPipeline?.id);
    setTabIndex(1);
  };

  const stopPreview = async () => {
    await updatePipeline({ stop: 'immediate' });

    if (outputSource) {
      outputSource.close();
    }
  };

  const run = async () => {
    check();
    if (!(await pipelineIsValid())) {
      return;
    }
    onOpen();
  };

  const start = async () => {
    const { data, error } = await post('/v1/pipelines', {
      body: {
        name: options.name!,
        parallelism: options.parallelism!,
        query: queryInput,
        udfs: [
          {
            language: 'rust',
            definition: udfsInput,
          },
        ],
      },
    });

    if (data) {
      localStorage.removeItem('query');
      navigate(`/pipelines/${data.id}`);
    }

    if (error) {
      setStartError(formatError(error));
    }
  };

  const sources = connectionTables.filter(s => s.tableType == 'source');
  const sinks = connectionTables.filter(s => s.tableType == 'sink');

  const startPipelineModal = (
    <StartPipelineModal
      isOpen={isOpen}
      onClose={onClose}
      startError={startError}
      options={options}
      setOptions={setOptions}
      start={start}
    />
  );

  const catalogType = (name: string, tables: Array<ConnectionTable>) => {
    return (
      <Stack p={4}>
        <Text fontSize={'md'} pt={2} pb={4} fontWeight={'bold'}>
          {name.toUpperCase()}S
        </Text>
        <Stack spacing={4}>
          {tables.length == 0 ? (
            <Box overflowY="auto" overflowX="hidden">
              <Text>
                No {name}s have been created. Create one <Link to="/connections/new">here</Link>.
              </Text>
            </Box>
          ) : (
            <Box overflowY="auto" overflowX="hidden">
              <Catalog tables={tables} />
            </Box>
          )}
        </Stack>
      </Stack>
    );
  };

  // Since we only fetch the first page of connection tables,
  // display a warning if there are too many to be shown.
  let catalogTruncatedWarning = <></>;
  if (catalogTruncated) {
    catalogTruncatedWarning = (
      <Alert flexShrink={0} status="warning">
        <AlertIcon />
        <AlertDescription>
          The catalogue is too large to be shown in its entirety. Please see the Connections tab for
          the complete listing.
        </AlertDescription>
      </Alert>
    );
  }

  let catalog = (
    <Stack
      width={300}
      background="gray.900"
      p={2}
      spacing={2}
      pt={4}
      borderRight={'1px solid'}
      borderColor={'gray.500'}
      overflow={'auto'}
    >
      {catalogTruncatedWarning}
      {catalogType('Source', sources)}
      {catalogType('Sink', sinks)}

      <Spacer />
      <Box p={4} borderTop={'1px solid'} borderColor={'gray.500'}>
        Write SQL to create a streaming pipeline. See the{' '}
        <Link to={'http://doc.arroyo.dev/sql'}>SQL docs</Link> for details on Arroyo SQL.
      </Box>
    </Stack>
  );

  const previewing = job?.runningDesired && job?.state != 'Failed' && !job?.finishTime;

  let startPreviewButton = <></>;
  let stopPreviewButton = <></>;

  if (previewing) {
    stopPreviewButton = (
      <Button
        onClick={stopPreview}
        size="md"
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
        size="md"
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
      size="md"
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
    <Button size="md" colorScheme="green" onClick={run} borderRadius={2}>
      Start Pipeline
    </Button>
  );

  const buttonGroup = (
    <HStack spacing={4} p={4}>
      {checkButton}
      {startPreviewButton}
      {stopPreviewButton}
      {startPipelineButton}
    </HStack>
  );

  let previewPipelineTab = (
    <TabPanel height="100%" position="relative">
      <Text>Check your SQL to see the pipeline graph.</Text>
    </TabPanel>
  );

  if (pipelineGraph) {
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
          <PipelineGraphViewer
            graph={pipelineGraph}
            operatorMetricGroups={operatorMetricGroups}
            setActiveOperator={() => {}}
          />
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
      previewResultsTabContent = (
        <Flex>
          <Text marginRight={'2'}>Job status:</Text>
          <Badge>{job?.state}</Badge>
        </Flex>
      );
    }
  }

  const previewResultsTab = (
    <TabPanel overflowX="auto" flex={1} position="relative">
      {previewResultsTabContent}
    </TabPanel>
  );

  const errorsTab = (
    <TabPanel overflowX="auto" height="100%" position="relative">
      {hasErrors ? (
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
          <PaginatedContent
            pages={operatorErrorsPages}
            totalPages={operatorErrorsTotalPages}
            setMaxPages={setOperatorErrorsMaxPages}
            content={<OperatorErrors operatorErrors={operatorErrors} />}
            setCurrentData={setOperatorErrors}
          />
        </Box>
      ) : (
        <Text>Job errors will appear here.</Text>
      )}
    </TabPanel>
  );

  const previewTabsContent = (
    <TabPanels display={'flex'} flexDirection={'column'} flex={1}>
      {previewPipelineTab}
      {previewResultsTab}
      {errorsTab}
    </TabPanels>
  );

  const editorTabs = (
    <Flex direction={'column'} padding={5} pl={0} backgroundColor="#1e1e1e" height="100%">
      <Tabs display={'flex'} flexDirection={'column'} flex={1}>
        <TabList>
          <Tab>query.sql</Tab>
          <Tab>udfs.rs</Tab>
        </TabList>
        <TabPanels flex={1}>
          <TabPanel height={'100%'}>
            <CodeEditor query={queryInput} setQuery={updateQuery} />
          </TabPanel>
          <TabPanel height={'100%'}>
            <CodeEditor query={udfsInput} setQuery={updateUdf} language="rust" />
          </TabPanel>
        </TabPanels>
      </Tabs>
    </Flex>
  );

  let errorMessage;
  if (pipelineGraphError) {
    errorMessage = formatError(pipelineGraphError);
  } else if (job?.state == 'Failed') {
    errorMessage = 'Job failed. See "Errors" tab for more details.';
  } else {
    errorMessage = '';
  }

  let errorComponent = <></>;
  if (errorMessage) {
    errorComponent = (
      <div>
        <Alert status="error">
          <AlertIcon />
          <AlertDescription>
            <Text noOfLines={2} textOverflow={'ellipsis'} wordBreak={'break-all'}>
              {errorMessage}
            </Text>
          </AlertDescription>
        </Alert>
      </div>
    );
  }

  let previewCompletedComponent = <></>;
  if (job?.finishTime && !job?.failureMessage) {
    previewCompletedComponent = (
      <div>
        <Alert status="success">
          <AlertIcon />
          <AlertDescription>
            <Text>Preview completed</Text>
          </AlertDescription>
        </Alert>
      </div>
    );
  }

  const tabs = (
    <Tabs
      display={'flex'}
      flexDirection={'column'}
      index={tabIndex}
      onChange={i => setTabIndex(i)}
      flex={1}
    >
      <TabList>
        <Flex width={'100%'} justifyContent={'space-between'}>
          <Flex>
            <Tab>Pipeline</Tab>
            <Tab>
              <HStack>
                <Text>Results</Text>
                {previewing ? <Spinner size="xs" speed="0.9s" /> : null}
              </HStack>
            </Tab>
            <Tab>
              <Text>Errors</Text>
              {hasErrors && <Icon as={WarningIcon} color={'red.400'} ml={2} />}
            </Tab>
          </Flex>
          {buttonGroup}
        </Flex>
      </TabList>
      {previewTabsContent}
    </Tabs>
  );

  const panelResizer = (
    <PanelResizeHandle>
      <Flex justifyContent="center">
        <MdDragHandle color={'grey'} />
      </Flex>
    </PanelResizeHandle>
  );

  return (
    <Flex height={'100vh'}>
      <Flex>{catalog}</Flex>
      <Flex direction={'column'} flex={1} minWidth={0}>
        <PanelGroup autoSaveId={'create-pipeline-panels'} direction="vertical">
          <Panel minSize={20}>{editorTabs}</Panel>
          {panelResizer}
          <Panel minSize={20}>
            <Flex direction={'column'} height={'100%'}>
              {errorComponent}
              {previewCompletedComponent}
              {tabs}
            </Flex>
          </Panel>
        </PanelGroup>
      </Flex>
      {startPipelineModal}
    </Flex>
  );
}
