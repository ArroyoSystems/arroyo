import {
  Alert,
  AlertDescription,
  AlertIcon,
  Badge,
  Box,
  Code,
  Flex,
  HStack,
  Icon,
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
import React, { useContext, useEffect, useMemo, useRef, useState } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { PipelineGraphViewer } from './PipelineGraph';
import { SqlOptions } from '../../lib/types';
import {
  JobLogMessage,
  PipelineLocalUdf,
  post,
  useOperatorErrors,
  usePipeline,
  usePipelineJobs,
  useQueryValidation,
} from '../../lib/data_fetching';
import Loading from '../../components/Loading';
import OperatorErrors from '../../components/OperatorErrors';
import StartPipelineModal from '../../components/StartPipelineModal';
import { formatError } from '../../lib/util';
import PaginatedContent from '../../components/PaginatedContent';
import {
  ImperativePanelHandle,
  Panel,
  PanelGroup,
  PanelResizeHandle,
} from 'react-resizable-panels';
import { PipelineOutputs } from './PipelineOutputs';
import { TourContext, TourSteps } from '../../tour';
import CreatePipelineTourModal from '../../components/CreatePipelineTourModal';
import TourCompleteModal from '../../components/TourCompleteModal';
import SyntaxHighlighter from 'react-syntax-highlighter';
import { vs2015 } from 'react-syntax-highlighter/dist/esm/styles/hljs';
import PipelineEditorTabs from './PipelineEditorTabs';
import { LocalUdf, LocalUdfsContext } from '../../udf_state';
import UdfLabel from '../udfs/UdfLabel';
import { PiFileSqlDuotone, PiFunction, PiGraph } from 'react-icons/pi';
import { BiTable } from 'react-icons/bi';
import { IoWarningOutline } from 'react-icons/io5';
import { useNavbar } from '../../App';
import { FiDatabase } from 'react-icons/fi';
import CatalogTab from './CatalogTab';
import UdfsResourceTab from '../udfs/UdfsResourceTab';

function useQuery() {
  const { search } = useLocation();

  return useMemo(() => new URLSearchParams(search), [search]);
}

export interface PreviewOptions {
  enableSinks: boolean;
}

export function CreatePipeline() {
  const [pipelineId, setPipelineId] = useState<string | undefined>(undefined);
  const { updatePipeline } = usePipeline(pipelineId);
  const { jobs } = usePipelineJobs(pipelineId, true, 500);
  const job = jobs?.length ? jobs[0] : undefined;
  const { operatorErrorsPages, operatorErrorsTotalPages, setOperatorErrorsMaxPages } =
    useOperatorErrors(pipelineId, job?.id);
  const [operatorErrors, setOperatorErrors] = useState<JobLogMessage[]>([]);
  const [queryInput, setQueryInput] = useState<string>('');
  const [queryInputToCheck, setQueryInputToCheck] = useState<string | undefined>(undefined);
  const { isOpen, onOpen, onClose } = useDisclosure();
  const [options, setOptions] = useState<SqlOptions>({ parallelism: 1 });
  const navigate = useNavigate();
  const [startError, setStartError] = useState<string | null>(null);
  const [tabIndex, setTabIndex] = useState<number>(0);
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
  const [previewError, setPreviewError] = useState<string | undefined>(undefined);
  const hasUdfValidationErrors = localUdfs.some(u => u.errors?.length);
  const [resourcePanelTab, setResourcePanelTab] = useState(0);
  const [udfValidationApiError, setUdfValidationApiError] = useState<any | undefined>(undefined);
  const [validationInProgress, setValidationInProgress] = useState<boolean>(false);
  const [startingPreview, setStartingPreview] = useState<boolean>(false);

  const { tourActive, tourStep, setTourStep, disableTour } = useContext(TourContext);

  const [sidebarElement, setSidebarElement] = useState<'tables' | 'udfs' | null>('tables');
  const [previewOptions, setPreviewOptions] = useState<PreviewOptions>({ enableSinks: false });

  const { setMenuItems } = useNavbar();

  const hasValidationErrors = queryValidation?.errors?.length || hasUdfValidationErrors;

  useEffect(() => {
    setMenuItems([
      {
        label: 'Tables',
        icon: FiDatabase,
        onClick: () => setSidebarElement(sidebarElement == 'tables' ? null : 'tables'),
        selected: sidebarElement == 'tables',
      },
      {
        label: 'UDFs',
        icon: PiFunction,
        onClick: () => setSidebarElement(sidebarElement == 'udfs' ? null : 'udfs'),
        selected: sidebarElement == 'udfs',
      },
    ]);
  }, [sidebarElement]);

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
              language: u.language!,
              open: false,
              errors: [],
            };
          });

          // merge with local udfs
          let merged = localUdfs;
          for (const udf of udfs) {
            const { data: udfValidation } = await post('/v1/udfs/validate', {
              body: {
                definition: udf.definition,
                language: udf.language,
              },
            });

            if (udfValidation) {
              udf.name = udfValidation.udfName ?? udf.name;
              udf.errors = udfValidation.errors ?? [];
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

  const panelRef = useRef<ImperativePanelHandle | null>(null);

  useEffect(() => {
    if (panelRef?.current != undefined) {
      if (sidebarElement == null && panelRef?.current?.getSize() > 0) {
        panelRef?.current?.collapse();
      } else if (sidebarElement != null && panelRef?.current?.getSize() == 0) {
        panelRef?.current?.expand();
      }
    }
  }, [sidebarElement]);

  // Top-level loading state
  if (copyFromLoading || !localUdfs) {
    return <Loading />;
  }

  const udfsValid = async () => {
    let valid = true;
    let newUdfs = [];
    for (const udf of localUdfs) {
      const { data: udfsValiation, error: udfsValiationError } = await post('/v1/udfs/validate', {
        body: {
          definition: udf.definition,
          language: udf.language,
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
      language: u.language,
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
    setPipelineId(undefined);
    setQueryInputToCheck(queryInput);
    setLocalUdfsToCheck(localUdfs);
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
    setStartingPreview(true);
    setPreviewError(undefined);

    if (!(await pipelineIsValid(1))) {
      setStartingPreview(false);
      return;
    }

    const udfs: PipelineLocalUdf[] = localUdfs.map(u => ({
      definition: u.definition,
      language: u.language,
    }));

    const { data: newPipeline, error } = await post('/v1/pipelines/preview', {
      body: {
        query: queryInput,
        udfs,
        enableSinks: previewOptions.enableSinks,
      },
    });

    setStartingPreview(false);

    if (error) {
      console.error('Create pipeline failed', error);
      setPreviewError(`Failed to start preview: ${formatError(error)}`);
    } else {
      // Setting the pipeline id will trigger fetching the job and subscribing to the output
      setPipelineId(newPipeline?.id);
    }
  };

  const stopPreview = async () => {
    await updatePipeline({ stop: 'immediate' });
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
      language: u.language,
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
          <PipelineGraphViewer graph={queryValidation.graph} setActiveOperator={() => {}} />
        </Box>
      </TabPanel>
    );
  }

  let previewResultsTabContent = <Text>Preview your SQL to see outputs.</Text>;
  const previewing = job?.runningDesired && job?.state != 'Failed' && !job?.finishTime;

  if (pipelineId != null && jobs != null && jobs[0] != null) {
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
        <PipelineOutputs pipelineId={pipelineId} job={jobs[0]} onDemand={false} />
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
    <Alert status="error" height={8}>
      <AlertIcon boxSize={4} />
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
              const content = u.errors!.join('\n').replaceAll('\\n', '\n');
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
      <TabPanel overflowX="auto" p={0} pl={2} height="100%" position="relative">
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
        <Text color={'gray.500'}>No job errors</Text>
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

  let errorMessage;
  if (queryValidationError) {
    errorMessage = formatError(queryValidationError);
  } else if (udfValidationApiError) {
    errorMessage = formatError(udfValidationApiError);
  } else if (job?.state == 'Failed') {
    errorMessage = job.failureMessage ?? 'Job failed.';
  } else if (previewError) {
    errorMessage = (
      <Code backgroundColor={'transparent'}>
        <pre>{previewError}</pre>
      </Code>
    );
  } else {
    errorMessage = '';
  }

  let errorComponent = <></>;
  if (errorMessage) {
    errorComponent = (
      <Alert status="error">
        <AlertIcon />
        <AlertDescription>
          <Text noOfLines={2} textOverflow={'ellipsis'} wordBreak={'break-all'}>
            {errorMessage}
          </Text>
        </AlertDescription>
      </Alert>
    );
  }

  let previewCompletedComponent = <></>;
  if (job?.finishTime && !job?.failureMessage) {
    previewCompletedComponent = (
      <Alert status="success" h={8}>
        <AlertIcon boxSize={4} />
        <AlertDescription>
          <Text>Preview completed</Text>
        </AlertDescription>
      </Alert>
    );
  }

  const tabs = (
    <Tabs
      display={'flex'}
      flexDirection={'row'}
      index={tabIndex}
      onChange={i => setTabIndex(i)}
      flex={1}
      overflow={'auto'}
      orientation="vertical"
      variant={'unstyled'}
      size={'md'}
      colorScheme="green"
    >
      <TabList>
        <Stack w={10} mx={2} spacing={4} h={'100%'} py={4}>
          <Tab
            title="Pipeline graph"
            borderRadius={'md'}
            _selected={{ bg: 'gray.600' }}
            _hover={{ bg: 'gray.500 ' }}
          >
            <Icon as={PiGraph} boxSize={5} />
          </Tab>
          <Tab
            title="Results"
            borderRadius={'md'}
            _selected={{ bg: 'gray.600' }}
            _hover={{ bg: 'gray.500 ' }}
          >
            <HStack>
              {previewing ? <Spinner size="xs" speed="0.9s" /> : <Icon boxSize={5} as={BiTable} />}
            </HStack>
          </Tab>
          <Tab
            title="Errors"
            borderRadius={'md'}
            _selected={{ bg: 'gray.600' }}
            _hover={{ bg: 'gray.500 ' }}
          >
            <Icon
              as={IoWarningOutline}
              boxSize={5}
              color={hasValidationErrors || hasOperatorErrors ? 'red.400' : undefined}
            />
          </Tab>
        </Stack>
      </TabList>
      {previewTabsContent}
    </Tabs>
  );

  return (
    <Flex height={'100vh'}>
      <PanelGroup direction="horizontal">
        <Panel minSize={0} defaultSize={15} collapsible={true} ref={panelRef}>
          <Flex h={'100vh'} overflowY={'auto'} p={4} bgColor={'#1A1A1A'}>
            {sidebarElement == 'tables' ? (
              <CatalogTab />
            ) : sidebarElement == 'udfs' ? (
              <UdfsResourceTab />
            ) : null}
          </Flex>
        </Panel>
        {sidebarElement != null && (
          <PanelResizeHandle style={{ width: '4px', backgroundColor: '#111' }} />
        )}
        <Panel minSize={60}>
          <Flex direction={'column'} bg={'#151515'} h={'100vh'}>
            <PanelGroup autoSaveId={'create-pipeline-panels'} direction="vertical">
              <Panel minSize={20}>
                <PipelineEditorTabs
                  queryInput={queryInput}
                  previewing={previewing}
                  startingPreview={startingPreview}
                  preview={preview}
                  stopPreview={stopPreview}
                  run={run}
                  pipelineIsValid={pipelineIsValid}
                  updateQuery={updateQuery}
                  previewOptions={previewOptions}
                  setPreviewOptions={setPreviewOptions}
                  job={job}
                />
              </Panel>
              <PanelResizeHandle style={{ height: '4px', backgroundColor: '#111' }} />
              <Panel minSize={20}>
                <Flex direction={'column'} height={'100%'}>
                  {errorComponent}
                  {tabs}
                  {hasValidationErrors ? validationErrorAlert : previewCompletedComponent}
                </Flex>
              </Panel>
            </PanelGroup>
          </Flex>
        </Panel>
      </PanelGroup>
      <CreatePipelineTourModal />
      <TourCompleteModal />
      {startPipelineModal}
    </Flex>
  );
}
