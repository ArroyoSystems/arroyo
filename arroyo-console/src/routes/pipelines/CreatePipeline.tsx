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
  Popover,
  PopoverArrow,
  PopoverBody,
  PopoverCloseButton,
  PopoverContent,
  PopoverHeader,
  PopoverTrigger,
  Spinner,
  Tab,
  TabList,
  TabPanel,
  TabPanels,
  Tabs,
  Text,
  useDisclosure,
} from '@chakra-ui/react';
import React, { useContext, useEffect, useMemo, useState } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { PipelineGraphViewer } from './PipelineGraph';
import { SqlOptions } from '../../lib/types';
import {
  JobLogMessage,
  OutputData,
  PipelineLocalUdf,
  post,
  useConnectionTables,
  useJobMetrics,
  useJobOutput,
  useOperatorErrors,
  usePipeline,
  usePipelineJobs,
  useQueryValidation,
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
import { TourContext, TourSteps } from '../../tour';
import CreatePipelineTourModal from '../../components/CreatePipelineTourModal';
import TourCompleteModal from '../../components/TourCompleteModal';
import SyntaxHighlighter from 'react-syntax-highlighter';
import { vs2015 } from 'react-syntax-highlighter/dist/esm/styles/hljs';
import PipelineEditorTabs from './PipelineEditorTabs';
import ResourcePanel from './ResourcePanel';
import useLocalStorage from 'use-local-storage';
import { LocalUdf, LocalUdfsContext } from '../../udf_state';
import UdfLabel from '../udfs/UdfLabel';
import { PiFileSqlDuotone } from 'react-icons/pi';

function useQuery() {
  const { search } = useLocation();

  return useMemo(() => new URLSearchParams(search), [search]);
}

export function CreatePipeline() {
  const [pipelineId, setPipelineId] = useState<string | undefined>(undefined);
  const { updatePipeline } = usePipeline(pipelineId);
  const { jobs } = usePipelineJobs(pipelineId, true);
  const job = jobs?.length ? jobs[0] : undefined;
  const { operatorErrorsPages, operatorErrorsTotalPages, setOperatorErrorsMaxPages } =
    useOperatorErrors(pipelineId, job?.id);
  const [operatorErrors, setOperatorErrors] = useState<JobLogMessage[]>([]);
  const [queryInput, setQueryInput] = useState<string>('');
  const [queryInputToCheck, setQueryInputToCheck] = useState<string>('');
  const { operatorMetricGroups } = useJobMetrics(pipelineId, job?.id);
  const { isOpen, onOpen, onClose } = useDisclosure();
  const [options, setOptions] = useState<SqlOptions>({ parallelism: 4, checkpointMS: 5000 });
  const navigate = useNavigate();
  const [startError, setStartError] = useState<string | null>(null);
  const [tabIndex, setTabIndex] = useState<number>(0);
  const [outputSource, setOutputSource] = useState<EventSource | undefined>(undefined);
  const [outputs, setOutputs] = useState<Array<{ id: number; data: OutputData }>>([]);
  const { connectionTablesLoading } = useConnectionTables(50);
  const queryParams = useQuery();
  const { pipeline: copyFrom, pipelineLoading: copyFromLoading } = usePipeline(
    queryParams.get('from') ?? undefined
  );
  const hasOperatorErrors = operatorErrorsPages?.length && operatorErrorsPages[0].data.length > 0;
  const { localUdfs, setLocalUdfs, openTab } = useContext(LocalUdfsContext);
  const [localUdfsToCheck, setLocalUdfsToCheck] = useState<LocalUdf[]>([]);
  const { queryValidation, queryValidationError, queryValidationLoading } = useQueryValidation(
    queryInputToCheck,
    localUdfsToCheck
  );
  const hasUdfValidationErrors = localUdfs.some(u => u.errors?.length);
  const hasValidationErrors = queryValidation?.errors?.length || hasUdfValidationErrors;
  const [resourcePanelTab, setResourcePanelTab] = useLocalStorage('resourcePanelTabIndex', 0);
  const [udfValidationApiError, setUdfValidationApiError] = useState<any | undefined>(undefined);
  const [validationInProgress, setValidationInProgress] = useState<boolean>(false);

  const { tourActive, tourStep, setTourStep, disableTour } = useContext(TourContext);

  useEffect(() => {
    if (tourActive) {
      setTourStep(TourSteps.CreatePipelineModal);
    }
  }, []);

  const updateQuery = (query: string) => {
    window.localStorage.setItem('query', query);
    setQueryInput(query);
  };

  useEffect(() => {
    const copyFromPipeline = async () => {
      let savedQuery = window.localStorage.getItem('query');
      if (copyFrom != null) {
        setQueryInput(copyFrom.query || '');
        if (copyFrom.udfs.length) {
          const udfs: LocalUdf[] = copyFrom.udfs.map(u => {
            const name = randomUdfName();
            return {
              id: name,
              name, // this gets updated after validation
              definition: u.definition,
              open: false,
              errors: [],
            };
          });

          // merge with local udfs
          let merged = localUdfs;
          for (const udf of udfs) {
            const { data: udfValiation } = await post('/v1/udfs/validate', {
              body: {
                definition: udf.definition,
              },
            });

            if (udfValiation) {
              udf.name = udfValiation.udfName ?? udf.name;
              udf.errors = udfValiation.errors ?? [];
            }

            if (!merged.some(u => u.name == udf.name)) {
              merged.push(udf);
            }
          }

          setLocalUdfs(merged);
        }
        setOptions({
          ...options,
          name: copyFrom.name + '-copy',
        });
      } else {
        if (savedQuery != null) {
          setQueryInput(savedQuery);
        }
      }
    };

    copyFromPipeline();
  }, [copyFrom]);

  const randomUdfName = () => {
    const id = Math.random().toString(36).substring(7);
    return `udf_${id}`;
  };

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
    if (pipelineId && job) {
      if (outputSource) {
        outputSource.close();
      }
      setOutputSource(useJobOutput(sseHandler, pipelineId, job.id));
    }
  }, [job?.id]);

  // Top-level loading state
  if (copyFromLoading || connectionTablesLoading || !localUdfs) {
    return <Loading />;
  }

  const udfsValid = async () => {
    let valid = true;
    let newUdfs = [];
    for (const udf of localUdfs) {
      const { data: udfsValiation, error: udfsValiationError } = await post('/v1/udfs/validate', {
        body: {
          definition: udf.definition,
        },
      });

      if (udfsValiation?.errors?.length) {
        valid = false;
      }

      if (udfsValiation) {
        newUdfs.push({
          ...udf,
          name: udfsValiation.udfName ?? udf.name,
          errors: udfsValiation.errors ?? [],
        });
      } else {
        valid = false;
        newUdfs.push(udf);
        setUdfValidationApiError(udfsValiationError);
      }
    }
    setLocalUdfs(newUdfs);
    return valid;
  };

  const queryValid = async () => {
    const udfs: PipelineLocalUdf[] = localUdfs.map(u => ({
      definition: u.definition,
    }));
    const { data: queryValidation } = await post('/v1/pipelines/validate_query', {
      body: {
        query: queryInput,
        udfs,
      },
    });

    if (queryValidation?.graph) {
      return true;
    }
    return false;
  };

  const pipelineIsValid = async (successTab?: number) => {
    // Setting this state triggers the uswSWR calls
    setQueryInputToCheck(queryInput);
    setLocalUdfsToCheck(localUdfs);
    setOutputs([]);
    setUdfValidationApiError(undefined);
    await stopPreview();

    // do synchronous api calls here
    setValidationInProgress(true);
    const valid = (await udfsValid()) && (await queryValid());
    if (valid) {
      if (successTab != undefined) {
        setTabIndex(successTab);
      }
    } else {
      setTabIndex(2);
    }
    setValidationInProgress(false);
    return valid;
  };

  const preview = async () => {
    setTourStep(undefined);
    setQueryInputToCheck('');
    setPipelineId(undefined);

    if (!(await pipelineIsValid(1))) {
      return;
    }

    const udfs: PipelineLocalUdf[] = localUdfs.map(u => ({
      definition: u.definition,
    }));

    const { data: newPipeline, error } = await post('/v1/pipelines', {
      body: {
        name: `preview-${new Date().getTime()}`,
        parallelism: 1,
        preview: true,
        query: queryInput,
        udfs,
      },
    });

    if (error) {
      console.log('Create pipeline failed');
    }

    // Setting the pipeline id will trigger fetching the job and subscribing to the output
    setPipelineId(newPipeline?.id);
  };

  const stopPreview = async () => {
    await updatePipeline({ stop: 'immediate' });

    if (outputSource) {
      outputSource.close();
    }
  };

  const run = async () => {
    if (!(await pipelineIsValid())) {
      return;
    }
    onOpen();
  };

  const start = async () => {
    console.log('starting');
    const udfs: PipelineLocalUdf[] = localUdfs.map(u => ({
      definition: u.definition,
    }));

    const { data, error } = await post('/v1/pipelines', {
      body: {
        name: options.name!,
        parallelism: options.parallelism!,
        query: queryInput,
        udfs,
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
      onClick={() => pipelineIsValid(0)}
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
      <Popover
        isOpen={tourStep == TourSteps.Preview}
        placement={'top'}
        closeOnBlur={false}
        variant={'tour'}
      >
        <PopoverTrigger>{startPreviewButton}</PopoverTrigger>
        <PopoverContent>
          <PopoverArrow />
          <PopoverCloseButton onClick={disableTour} />
          <PopoverHeader>Nice!</PopoverHeader>
          <PopoverBody>
            Finally, run a preview pipeline to see the results of your query.
          </PopoverBody>
        </PopoverContent>
      </Popover>
      {stopPreviewButton}
      {startPipelineButton}
    </HStack>
  );

  let previewPipelineTab = (
    <TabPanel height="100%" position="relative">
      <Text>Check your SQL to see the pipeline graph.</Text>
    </TabPanel>
  );

  if (queryValidation?.graph) {
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
            graph={queryValidation.graph}
            operatorMetricGroups={operatorMetricGroups}
            setActiveOperator={() => {}}
          />
        </Box>
      </TabPanel>
    );
  }

  let previewResultsTabContent = <Text>Preview your SQL to see outputs.</Text>;

  if (outputs.length) {
    setTourStep(TourSteps.TourCompleted);
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

  let previewResultsTab = (
    <TabPanel overflowX="auto" flex={1} position="relative">
      {previewResultsTabContent}
    </TabPanel>
  );

  const validationErrorAlert = (
    <Alert status="error">
      <AlertIcon />
      <AlertDescription>
        <Text>Validation error</Text>
      </AlertDescription>
    </Alert>
  );

  let errorsTab;
  if (hasValidationErrors) {
    let queryErrors = <></>;
    let udfErrors = <></>;

    if (queryValidation?.errors) {
      queryErrors = (
        <Flex flexDirection={'column'} py={3} gap={2}>
          <Flex gap={1} onClick={() => openTab('query')} cursor={'pointer'} w={'min-content'}>
            <Icon as={PiFileSqlDuotone} boxSize={5} />
            <Text>Query</Text>
          </Flex>
          <SyntaxHighlighter language="text" style={vs2015} customStyle={{ borderRadius: '5px' }}>
            {queryValidation.errors[0]}
          </SyntaxHighlighter>
        </Flex>
      );
    }

    if (hasUdfValidationErrors) {
      udfErrors = (
        <Box>
          {localUdfs
            .filter(u => u.errors?.length)
            .map(u => {
              const text = `"${u.errors!.join('\\n')}"`;
              let content = text;
              try {
                content = JSON.parse(text);
              } catch (e) {}
              return (
                <Flex key={u.id} flexDirection={'column'} py={3} gap={2}>
                  <UdfLabel udf={u} />
                  <SyntaxHighlighter
                    language="text"
                    style={vs2015}
                    customStyle={{ borderRadius: '5px' }}
                  >
                    {content}
                  </SyntaxHighlighter>
                </Flex>
              );
            })}
        </Box>
      );
    }

    errorsTab = (
      <TabPanel overflowX="auto" height="100%" position="relative">
        {validationErrorAlert}
        {queryErrors}
        {udfErrors}
      </TabPanel>
    );
  } else if (hasOperatorErrors) {
    errorsTab = (
      <TabPanel overflowX="auto" height="100%" position="relative">
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
      </TabPanel>
    );
  } else {
    errorsTab = (
      <TabPanel overflowX="auto" height="100%" position="relative">
        <Text>Compilation and job errors will appear here.</Text>
      </TabPanel>
    );
  }

  const loadingTab = (
    <TabPanel overflowX="auto" height="100%" position="relative">
      <Loading />
    </TabPanel>
  );

  if (validationInProgress || queryValidationLoading) {
    previewPipelineTab = loadingTab;
    previewResultsTab = loadingTab;
    errorsTab = loadingTab;
  }

  const previewTabsContent = (
    <TabPanels display={'flex'} flexDirection={'column'} flex={1} minHeight={0}>
      {previewPipelineTab}
      {previewResultsTab}
      {errorsTab}
    </TabPanels>
  );

  const editorTabs = <PipelineEditorTabs queryInput={queryInput} updateQuery={updateQuery} />;

  let errorMessage;
  if (queryValidationError) {
    errorMessage = formatError(queryValidationError);
  } else if (udfValidationApiError) {
    errorMessage = formatError(udfValidationApiError);
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

  let errorsTabIcon = <></>;
  if (hasValidationErrors || hasOperatorErrors) {
    errorsTabIcon = <Icon as={WarningIcon} color={'red.400'} ml={2} />;
  }

  const tabs = (
    <Tabs
      display={'flex'}
      flexDirection={'column'}
      index={tabIndex}
      onChange={i => setTabIndex(i)}
      flex={1}
      overflow={'auto'}
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
              {errorsTabIcon}
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
      <Flex>
        <ResourcePanel tabIndex={resourcePanelTab} handleTabChange={setResourcePanelTab} />
      </Flex>
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
      <CreatePipelineTourModal />
      <TourCompleteModal />
      {startPipelineModal}
    </Flex>
  );
}
