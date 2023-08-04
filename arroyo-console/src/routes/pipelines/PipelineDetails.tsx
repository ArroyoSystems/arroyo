import './pipelines.css';

import { useParams } from 'react-router-dom';
import {
  Alert,
  AlertDescription,
  AlertIcon,
  AlertTitle,
  Badge,
  Box,
  Button,
  ButtonGroup,
  Flex,
  Heading,
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
import React, { useState } from 'react';
import 'reactflow/dist/style.css';
import 'metrics-graphics/dist/mg.css';
import { ApiClient } from '../../main';
import { CodeEditor } from './SqlEditor';
import PipelineConfigModal from './PipelineConfigModal';
import {
  OutputData,
  StopType,
  useJobCheckpoints,
  useJobOutput,
  useJobMetrics,
  useOperatorErrors,
  usePipeline,
  usePipelineJobs,
} from '../../lib/data_fetching';
import OperatorDetail from '../../components/OperatorDetail';
import Checkpoints from '../../components/Checkpoints';
import Loading from '../../components/Loading';
import OperatorErrors from '../../components/OperatorErrors';
import { PipelineGraphViewer } from './PipelineGraph';
import PipelineNotFound from '../../components/PipelineNotFound';
import { QuestionOutlineIcon } from '@chakra-ui/icons';
import { formatError } from '../../lib/util';
import { PipelineOutputs } from './PipelineOutputs';

export function PipelineDetails({ client }: { client: ApiClient }) {
  const [activeOperator, setActiveOperator] = useState<string | undefined>(undefined);
  const [outputs, setOutputs] = useState<Array<{ id: number; data: OutputData }>>([]);
  const [subscribed, setSubscribed] = useState<boolean>(false);
  const {
    isOpen: configModalOpen,
    onOpen: onConfigModalOpen,
    onClose: onConfigModalClose,
  } = useDisclosure();

  let { pipelineId: id } = useParams();

  const { pipeline, pipelineError, updatePipeline } = usePipeline(id, true);
  const { jobs, jobsError } = usePipelineJobs(id, true);
  const job = jobs?.length ? jobs[0] : undefined;
  const { checkpoints } = useJobCheckpoints(id, job?.id);
  const { operatorErrors } = useOperatorErrors(id, job?.id);
  const { operatorMetricGroups } = useJobMetrics(id, job?.id);

  if (pipelineError || jobsError) {
    return (
      <PipelineNotFound
        icon={<QuestionOutlineIcon boxSize={55} />}
        message={formatError(pipelineError ?? jobsError)}
      />
    );
  }

  if (!pipeline || !job) {
    return <Loading />;
  }

  const sseHandler = (event: MessageEvent) => {
    const parsed = JSON.parse(event.data) as OutputData;
    outputs.push({ id: Number(event.lastEventId), data: parsed });
    if (outputs.length > 20) {
      outputs.shift();
    }
    setOutputs(outputs);
  };

  const subscribe = async () => {
    if (subscribed) {
      return;
    }

    setSubscribed(true);
    useJobOutput(sseHandler, pipeline.id, job?.id);
  };

  async function updateJobState(stop: StopType) {
    console.log(`Setting pipeline stop_mode=${stop}`);
    updatePipeline({ stop });
  }

  async function updateJobParallelism(parallelism: number) {
    console.log(`Setting pipeline parallelism=${parallelism}`);
    updatePipeline({ parallelism });
  }

  let operatorDetail = undefined;
  if (activeOperator) {
    operatorDetail = (
      <OperatorDetail pipelineId={pipeline.id} jobId={job.id} operatorId={activeOperator} />
    );
  }

  const operatorsTab = (
    <TabPanel display={'flex'} height={'100%'}>
      <Flex flex="1" height={'100%'}>
        <PipelineGraphViewer
          graph={pipeline.graph}
          operatorMetricGroups={operatorMetricGroups}
          setActiveOperator={setActiveOperator}
          activeOperator={activeOperator}
        />
      </Flex>
      <Stack w="500px" className="pipelineInfo" spacing={2} overflow={'auto'}>
        {job?.failureMessage ? (
          <Box>
            <Alert status="error" marginBottom={5}>
              <Box>
                <AlertTitle>Job Failed</AlertTitle>
                <AlertDescription>{job?.failureMessage}</AlertDescription>
              </Box>
              <Spacer />
              <AlertIcon alignSelf="flex-start" />
            </Alert>
          </Box>
        ) : null}

        <Box className="field">
          <Box className="fieldName">Pipeline ID</Box>
          <Box className="fieldValue">{pipeline.id}</Box>
        </Box>
        <Box className="field">
          <Box className="fieldName">Job ID</Box>
          <Box className="fieldValue">{job.id}</Box>
        </Box>
        <Box className="field">
          <Box className="fieldName">State</Box>
          <Box className="fieldValue">{job.state}</Box>
        </Box>
        {operatorDetail}
      </Stack>
    </TabPanel>
  );

  const outputsTab = (
    <TabPanel w={'100%'}>
      {outputs.length == 0 ? (
        pipeline.graph.nodes.find(n => n.operator.includes('WebSink')) != null ? (
          <Button isLoading={subscribed} onClick={subscribe} width={150} size="sm">
            Read output
          </Button>
        ) : (
          <Text>Pipeline does not have a web sink</Text>
        )
      ) : (
        <PipelineOutputs outputs={outputs} />
      )}
    </TabPanel>
  );

  const checkpointsTab = (
    <TabPanel>
      {
        <Checkpoints
          client={client}
          pipeline={pipeline}
          job={job}
          checkpoints={checkpoints ?? []}
        />
      }
    </TabPanel>
  );

  const queryTab = (
    <TabPanel w={'100%'}>
      <Box>
        <CodeEditor query={pipeline.query} readOnly={true} />
      </Box>
    </TabPanel>
  );

  const udfsTab = (
    <TabPanel w={'100%'}>
      <Box>
        <CodeEditor query={pipeline.udfs[0]?.definition || ''} language="rust" readOnly={true} />
      </Box>
    </TabPanel>
  );

  const errorsTab = (
    <TabPanel padding={5}>
      <OperatorErrors operatorErrors={operatorErrors} />
    </TabPanel>
  );

  const inner = (
    <Tabs display={'Flex'} flexDirection={'column'} width={'100%'}>
      <TabList>
        <Tab>Operators</Tab>
        <Tab>Outputs</Tab>
        <Tab>Checkpoints</Tab>
        <Tab>Query</Tab>
        <Tab>UDFs</Tab>
        <Tab>
          Errors{' '}
          {(operatorErrors?.length || 0) > 0 && (
            <Badge ml={2} colorScheme="red" size={'xs'}>
              {operatorErrors!.length}
            </Badge>
          )}
        </Tab>
      </TabList>
      <Flex minH={0} flex={1}>
        <TabPanels>
          {operatorsTab}
          {outputsTab}
          {checkpointsTab}
          {queryTab}
          {udfsTab}
          {errorsTab}
        </TabPanels>
      </Flex>
    </Tabs>
  );

  let configModal = <></>;
  if (pipeline.graph.nodes) {
    const parallelism = Math.max(...pipeline.graph.nodes.map(({ parallelism }) => parallelism));

    configModal = (
      <PipelineConfigModal
        parallelism={parallelism}
        isOpen={configModalOpen}
        onClose={onConfigModalClose}
        updateJobParallelism={updateJobParallelism}
      />
    );
  }

  let editPipelineButton = <></>;
  let actionButton = <></>;
  if (pipeline) {
    editPipelineButton = <Button onClick={onConfigModalOpen}>Edit</Button>;
    actionButton = (
      <Button
        isDisabled={pipeline.action == null}
        onClick={async () => {
          await updateJobState(pipeline.action!);
        }}
      >
        {pipeline.actionInProgress ? <Spinner size="xs" mr={2} /> : null}
        {pipeline.actionText}
      </Button>
    );
  }

  const headerArea = (
    <Flex>
      <Box p={5}>
        <Heading as="h4" size="md">
          {pipeline?.name} <Badge>{job?.state}</Badge>
        </Heading>
      </Box>
      <Spacer />
      <Box p={5}>
        <ButtonGroup>
          {editPipelineButton}
          {actionButton}
        </ButtonGroup>
      </Box>
    </Flex>
  );

  return (
    <Flex height={'100vh'} flexDirection={'column'}>
      {headerArea}
      <Flex flexGrow={1} minHeight={'0'}>
        {inner}
      </Flex>
      {configModal}
    </Flex>
  );
}
