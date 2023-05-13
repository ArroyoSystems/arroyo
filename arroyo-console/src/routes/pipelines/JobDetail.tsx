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
  Code,
  Flex,
  Heading,
  HStack,
  Link,
  ListItem,
  Spacer,
  Stack,
  Stat,
  StatGroup,
  StatLabel,
  StatNumber,
  Tab,
  TabList,
  TabPanel,
  TabPanels,
  Tabs,
  Text,
  theme,
  UnorderedList,
  useDisclosure,
} from '@chakra-ui/react';
import React, { ReactNode, useEffect, useState } from 'react';
import {
  CheckpointDetailsReq,
  CheckpointDetailsResp,
  CheckpointOverview,
  GrpcOutputSubscription,
  JobCheckpointsReq,
  JobDetailsReq,
  JobDetailsResp,
  JobGraph,
  JobMetricsReq,
  JobMetricsResp,
  OperatorCheckpointDetail,
  OutputData,
  StopType,
  TaskCheckpointEventType,
} from '../../gen/api_pb';
import 'reactflow/dist/style.css';
import * as MG from 'metrics-graphics';
import 'metrics-graphics/dist/mg.css';
import * as d3 from 'd3';
import * as util from '../../lib/util';
import { PipelineGraph } from './JobGraph';
import { ApiClient } from '../../main';
import { PipelineOutputs } from './JobOutputs';
import { CodeEditor } from './SqlEditor';
import PipelineConfigModal from './PipelineConfigModal';

interface JobDetailState {
  pipeline?: JobDetailsResp;
}

interface JobMetrics {
  metrics?: JobMetricsResp;
}

export function JobDetail({ client }: { client: ApiClient }) {
  const [state, setState] = React.useState<JobDetailState>({});
  const [metrics, setMetrics] = useState<JobMetrics>({});
  const [activeOperator, setActiveOperator] = useState<string | undefined>(undefined);
  const [outputs, setOutputs] = useState<Array<{ id: number; data: OutputData }>>([]);
  const [jobFetchInProgress, setJobFetchInProgress] = useState<boolean>(false);
  const [jobMetricsInProgress, setJobMetricsInProgress] = useState<boolean>(false);
  const [checkpoints, setCheckpoints] = useState<Array<CheckpointOverview>>([]);
  const [checkpointFetchInProgress, setCheckpointFetchInProgress] = useState<boolean>(false);
  const [subscribed, setSubscribed] = useState<boolean>(false);
  const {
    isOpen: configModalOpen,
    onOpen: onConfigModalOpen,
    onClose: onConfigModalClose,
  } = useDisclosure();

  let { id } = useParams();

  const fetchPipeline = async () => {
    if (!jobFetchInProgress) {
      setJobFetchInProgress(true);
      try {
        const response = await (
          await client()
        ).getJobDetails(
          new JobDetailsReq({
            jobId: id,
          })
        );

        setState({ pipeline: response });
      } catch (e) {
        console.log('Failed while fetching job status', e);
      }
      setJobFetchInProgress(false);
    }
  };

  const fetchMetrics = async () => {
    if (!jobMetricsInProgress) {
      setJobMetricsInProgress(true);
      const response = await (
        await client()
      ).getJobMetrics(
        new JobMetricsReq({
          jobId: id,
        })
      );

      setMetrics({ metrics: response });
      setJobMetricsInProgress(false);
    }
  };

  const fetchCheckpoints = async () => {
    if (!checkpointFetchInProgress) {
      setCheckpointFetchInProgress(true);
      const response = await (
        await client()
      ).getCheckpoints(
        new JobCheckpointsReq({
          jobId: id,
        })
      );

      setCheckpoints(response.checkpoints);
      setCheckpointFetchInProgress(false);
    }
  };

  useEffect(() => {
    fetchPipeline();
    fetchMetrics();
    fetchCheckpoints();
    const int1 = setInterval(fetchPipeline, 1000);
    const int2 = setInterval(fetchMetrics, 5000);
    const int3 = setInterval(fetchCheckpoints, 5000);
    return () => {
      clearInterval(int1);
      clearInterval(int2);
      clearInterval(int3);
    };
  }, []);

  const subscribe = async () => {
    if (subscribed) {
      return;
    }

    setSubscribed(true);

    let row = 1;
    for await (const res of (await client()).subscribeToOutput(
      new GrpcOutputSubscription({
        jobId: id,
      })
    )) {
      outputs.push({ id: row++, data: res });
      if (outputs.length > 20) {
        outputs.shift();
      }

      setOutputs(outputs);
    }
  };

  async function updateJobState(stop: StopType) {
    console.log(`Setting pipeline stop_mode=${stop}`);
    await (await client()).updateJob({ jobId: id, stop: stop });
    fetchPipeline();
  }

  async function updateJobParallelism(parallelism: number) {
    console.log(`Setting pipeline parallelism=${parallelism}`);
    await (
      await client()
    ).updateJob({
      jobId: id,
      parallelism,
    });
    fetchPipeline();
  }

  let inner;
  if (state.pipeline == null || state.pipeline.jobGraph == null) {
    inner = <p>Loading</p>;
  } else {
    let operatorDetail = undefined;
    if (activeOperator != undefined) {
      operatorDetail = (
        <OperatorDetail
          operator_id={activeOperator}
          graph={state.pipeline?.jobGraph}
          metrics={metrics.metrics}
        ></OperatorDetail>
      );
    }

    inner = (
      <Tabs h="100%">
        <TabList>
          <Tab>Operators</Tab>
          <Tab>Outputs</Tab>
          <Tab>Checkpoints</Tab>
          <Tab>Query</Tab>
          <Tab>UDFs</Tab>
        </TabList>
        <TabPanels h="100%">
          <TabPanel h="100%">
            <Flex h="100%">
              <Box flex="1">
                <PipelineGraph
                  graph={state.pipeline?.jobGraph}
                  setActiveOperator={setActiveOperator}
                  activeOperator={activeOperator}
                />
              </Box>
              <Stack w="500px" className="pipelineInfo" spacing={2}>
                {state.pipeline?.jobStatus?.failureMessage ? (
                  <Alert status="error" marginBottom={5}>
                    <Box>
                      <AlertTitle>Job Failed</AlertTitle>
                      <AlertDescription>
                        {state.pipeline?.jobStatus?.failureMessage}
                      </AlertDescription>
                    </Box>
                    <Spacer />
                    <AlertIcon alignSelf="flex-start" />
                  </Alert>
                ) : null}

                <Box className="field">
                  <Box className="fieldName">Name</Box>
                  <Box className="fieldValue">{state.pipeline?.jobStatus?.pipelineName}</Box>
                </Box>
                <Box className="field">
                  <Box className="fieldName">State</Box>
                  <Box className="fieldValue">{state.pipeline?.jobStatus?.state}</Box>
                </Box>
                {operatorDetail}
              </Stack>
            </Flex>
          </TabPanel>

          <TabPanel>
            {outputs.length == 0 ? (
              state.pipeline?.jobGraph?.nodes.find(n => n.operator.includes('GrpcSink')) != null ? (
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

          <TabPanel>
            {<Checkpoints job={state.pipeline} checkpoints={checkpoints} client={client} />}
          </TabPanel>

          <TabPanel>
            <Box>
              <CodeEditor query={state.pipeline?.jobStatus?.definition!} readOnly={true} />
            </Box>
          </TabPanel>

          <TabPanel>
            <Box>
              <CodeEditor
                query={(state.pipeline?.jobStatus?.udfs || [{ definition: '' }])[0].definition}
                language="rust"
                readOnly={true}
              />
            </Box>
          </TabPanel>
        </TabPanels>
      </Tabs>
    );
  }

  let configModal = <></>;
  if (state.pipeline?.jobGraph?.nodes) {
    const { nodes } = state.pipeline.jobGraph;
    const parallelism = Math.max(...nodes.map(({ parallelism }) => parallelism));

    configModal = (
      <PipelineConfigModal
        isOpen={configModalOpen}
        parallelism={parallelism}
        onClose={onConfigModalClose}
        updateJobParallelism={updateJobParallelism}
      />
    );
  }

  const editPipelineButton = <Button onClick={onConfigModalOpen}>Edit</Button>;

  const actionButton = (
    <Button
      isDisabled={state.pipeline?.action == undefined}
      isLoading={state.pipeline?.inProgress}
      loadingText={state.pipeline?.actionText}
      onClick={async () => {
        await updateJobState(state.pipeline?.action!);
      }}
    >
      {state.pipeline?.actionText}
    </Button>
  );

  return (
    <Box top={0} bottom={0} right={0} left={200} position="absolute" overflowY="hidden">
      <Flex>
        <Box p={5}>
          <Text fontSize={20}>
            Pipeline {id} <Badge>{state.pipeline?.jobStatus?.state}</Badge>
          </Text>
        </Box>
        <Spacer />
        <Box p={5}>
          <ButtonGroup>
            {editPipelineButton}
            {actionButton}
          </ButtonGroup>
        </Box>
      </Flex>
      {inner}
      {configModal}
    </Box>
  );
}

function OperatorDetail({
  operator_id,
  graph,
  metrics,
}: {
  operator_id: string;
  graph?: JobGraph;
  metrics?: JobMetricsResp;
}) {
  if (graph == undefined || metrics == undefined || metrics.metrics?.[operator_id] == undefined) {
    return <Box>loading...</Box>;
  } else {
    const node = graph.nodes.find(n => n.nodeId == operator_id);
    const node_metrics = metrics.metrics[operator_id];

    let msgRecv = 0;
    let msgSent = 0;
    let msgSentData;
    let msgRecvData;

    if (node_metrics != null && node_metrics.subtasks != null) {
      msgRecv = Object.values(node_metrics.subtasks)
        .map(n => n.messagesRecv)
        .reduce((s, a) => s + a[a.length - 1].value, 0);
      msgSent = Object.values(node_metrics.subtasks)
        .map(n => n.messagesSent)
        .reduce((s, a) => s + a[a.length - 1].value, 0);

      msgRecvData = Object.entries(node_metrics.subtasks).map(kv => {
        return kv[1].messagesRecv.map(m => {
          return {
            label: kv[0],
            x: new Date(Number(m.time) / 1000),
            y: m.value + m.value * Math.random() * 0.01,
          };
        });
      });

      msgSentData = Object.entries(node_metrics.subtasks).map(kv => {
        return kv[1].messagesSent.map(m => {
          return {
            label: kv[0],
            x: new Date(Number(m.time) / 1000),
            y: m.value + m.value * Math.random() * 0.01,
          };
        });
      });
    }

    return (
      <Box className="operatorDetail" marginTop={10} border="1px solid #333" padding="10px">
        <HStack fontWeight="semibold">
          <Box>operator</Box>
          <Spacer />
          <Box border="1px solid #aaa" px={1.5} fontSize={12} title="parallelism for this operator">
            {node?.parallelism}
          </Box>
        </HStack>
        <Box marginTop="10px">{node?.operator}</Box>
        <Box marginTop="10px" fontFamily="monaco,ubuntu mono,fixed-width">
          <Code>{Math.round(msgRecv)} eps</Code> rx
          <Code marginLeft="20px">{Math.round(msgSent)} eps</Code> tx
        </Box>
        <Box className="chart" marginTop="20px" fontSize={14}>
          Events RX
          <TimeSeriesGraph data={msgRecvData} timeWindowMs={5 * 60 * 1000} />
        </Box>

        <Box className="chart" marginTop="20px" fontSize={14}>
          Events TX
          <TimeSeriesGraph data={msgSentData} timeWindowMs={5 * 60 * 1000} />
        </Box>
      </Box>
    );
  }
}

interface TimeSeries {
  data?: Array<Array<{ x: Date; y: number }>>;
  timeWindowMs: number;
}

class TimeSeriesGraph extends React.Component<TimeSeries, {}> {
  private id = (Math.random() * 100000).toFixed();

  update() {
    if (this.props.data == null) {
      return;
    }
    const id = 'timeseries-graph-' + this.id;
    const now = new Date();
    const start = new Date(now.getTime() - this.props.timeWindowMs);

    const yMax =
      Math.round(
        this.props.data
          .flat()
          .map(f => f.y)
          .reduce((p, n) => Math.max(p, n), 0) *
          1.25 *
          10
      ) / 10;

    const yMin =
      Math.round(
        this.props.data
          .flat()
          .map(f => f.y)
          .reduce((p, n) => Math.min(p, n), Infinity) *
          0.75 *
          10
      ) / 10;

    // make sure we have a color for every series, otherwise the library defaults to black
    let colors = d3.schemeSet2.concat(
      new Array(Math.max(0, this.props.data.length - d3.schemeSet2.length)).fill('white')
    );

    const data = {
      data: this.props.data,
      target: document.getElementById(id),
      height: 200,
      width: 450,
      colors: colors,
      xAccessor: 'x',
      yAccessor: 'y',
      xScale: {
        minValue: start,
        maxValue: now,
      },
      yScale: {
        minValue: yMin,
        maxValue: yMax,
      },
      yAxis: {
        tickCount: 4,
      },
      xAxis: {
        tickCount: 5,
      },
      tooltipFunction: (data: { x: Date; y: number }) => {
        return `${new Intl.DateTimeFormat('en-US', { timeStyle: 'medium' }).format(data.x)} ${
          Math.round(data.y * 10) / 10
        }`;
      },
    };

    // @ts-ignore types are not general enough in MetricsGraphics
    new MG.LineChart(data);
  }

  componentDidMount() {
    this.update();
  }

  componentDidUpdate() {
    this.update();
  }

  render() {
    return <div className="timeseriesGraph" id={'timeseries-graph-' + this.id}></div>;
  }
}

function formatDurationHMS(micros: number): string {
  let millis = micros / 1000;
  let h = Math.floor(millis / 1000 / 60 / 60);
  let m = Math.floor((millis - h * 1000 * 60 * 60) / 1000 / 60);
  let s = Math.floor((millis - h * 1000 * 60 * 60 - m * 1000 * 60) / 1000);

  return `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}:${String(s).padStart(
    2,
    '0'
  )}`;
}

function OperatorCheckpoint({
  op,
  scale,
  w,
}: {
  op: { id: string; name: string; parallelism: number; checkpoint?: OperatorCheckpointDetail };
  scale: (x: number) => number;
  w: number;
}) {
  const [expanded, setExpanded] = useState<boolean>(false);

  let now = new Date().getTime() * 1000;

  let left = scale(Number(op.checkpoint?.startTime));
  let className = op.checkpoint?.finishTime == undefined ? 'in-progress' : 'finished';
  let finishTime = op.checkpoint?.finishTime == undefined ? now : Number(op.checkpoint?.finishTime);
  let width = Math.max(scale(finishTime) - left, 1);

  let tasks = <Box></Box>;

  if (expanded) {
    tasks = (
      <Box marginTop={6} marginLeft={1}>
        {Array(op.parallelism)
          .fill(1)
          .map((x, y) => x + y)
          .map(i => {
            let t = op.checkpoint?.tasks[i - 1];

            let inner: Array<ReactNode> = [];
            if (t != null) {
              let left = scale(Number(t.startTime));

              let alignmentTime = t.events.find(
                e => e.eventType == TaskCheckpointEventType.ALIGNMENT_STARTED
              )?.time;
              let alignmentLeft = scale(Number(alignmentTime));

              let checkpointStarted =
                t.events.find(e => e.eventType == TaskCheckpointEventType.CHECKPOINT_STARTED)
                  ?.time || now;
              let checkpointLeft = scale(Number(checkpointStarted));

              let syncTime = t.events.find(
                e => e.eventType == TaskCheckpointEventType.CHECKPOINT_SYNC_FINISHED
              )?.time;
              let syncLeft = syncTime == null ? null : scale(Number(syncTime));

              let className = Number(t.finishTime) == 0 ? 'in-progress' : 'finished';
              let finishTime = Number(t.finishTime) == 0 ? now : Number(t.finishTime);
              let width = Math.max(scale(finishTime) - left, 1);

              inner = [
                <Box
                  h={1}
                  className={className}
                  position="relative"
                  left={left}
                  width={width}
                ></Box>,
              ];

              //console.log(syncLeft, scale(now), alignmentLeft);

              inner.push(
                <Box
                  h={1}
                  backgroundColor="orange"
                  position="relative"
                  left={alignmentLeft}
                  width={Math.max((checkpointLeft || scale(now)) - alignmentLeft, 2)}
                />
              );

              if (syncLeft != null) {
                inner.push(
                  <Box
                    h={1}
                    backgroundColor={theme.colors.blue[400]}
                    position="relative"
                    left={checkpointLeft}
                    width={Math.max(syncLeft - checkpointLeft, 2)}
                  />
                );
              }
            }

            return (
              <Box marginTop={2} marginBottom={1} key={i} h={3} w={w} backgroundColor="#333">
                {inner}
              </Box>
            );
          })}
      </Box>
    );
  }

  return (
    <Flex
      marginTop="10px"
      textAlign="right"
      className="operator-checkpoint"
      onClick={() => setExpanded(!expanded)}
    >
      <Box width={200} marginRight="10px">
        <Text
          fontSize="sm"
          textOverflow="ellipsis"
          overflow="hidden"
          whiteSpace="nowrap"
          title={op.name}
        >
          {op.name}
        </Text>
      </Box>
      <Box width={w + 8} padding={0} position="relative" backgroundColor="#333">
        <Box
          className={className}
          title={util.durationFormat(finishTime - Number(op.checkpoint?.startTime))}
          position="absolute"
          margin={1}
          height={3}
          left={left}
          width={width}
        ></Box>
        {tasks}
      </Box>
    </Flex>
  );
}

function Checkpoints({
  job,
  checkpoints,
  client,
}: {
  job: JobDetailsResp;
  checkpoints: Array<CheckpointOverview>;
  client: ApiClient;
}) {
  const [epoch, setEpoch] = useState<number | null>(null);
  const [detailsFetchInProgress, setDetailsFetchInProgress] = useState<boolean>(false);
  const [checkpoint, setCheckpoint] = useState<CheckpointDetailsResp | null>(null);

  const fetchDetails = async () => {
    if (!detailsFetchInProgress && epoch != null) {
      setDetailsFetchInProgress(true);
      const response = await (
        await client()
      ).getCheckpointDetail(
        new CheckpointDetailsReq({
          jobId: job.jobStatus?.jobId,
          epoch: epoch,
        })
      );

      setCheckpoint(response);
      setDetailsFetchInProgress(false);
    }
  };

  useEffect(() => {
    fetchDetails();
    const int = setInterval(fetchDetails, 1000);
    return () => {
      clearInterval(int);
    };
  }, [epoch]);

  let details =
    checkpoints.length > 0 ? (
      <Text>Select checkpoint</Text>
    ) : (
      <Text textStyle="italic">No checkpoints</Text>
    );

  if (checkpoint != null) {
    let start = Number(checkpoint.overview?.startTime);
    let end = Number(
      checkpoint.overview?.finishTime != undefined
        ? checkpoint.overview?.finishTime
        : new Date().getTime() * 1000
    );

    let w = 500;

    function scale(x: number): number {
      return ((x - start) / (end - start)) * w;
    }

    let ops = job.jobGraph?.nodes
      .slice()
      .map(n => {
        return {
          id: n.nodeId,
          name: n.operator,
          parallelism: n.parallelism,
          checkpoint: checkpoint?.operators[n.nodeId],
        };
      })
      .sort((a, b) => Number(a.id.split('_')[1]) - Number(b.id.split('_')[1]));

    let checkpointBytes = Object.values(checkpoint.operators)
      .flatMap(op => Object.values(op.tasks).map(t => (t.bytes == null ? 0 : Number(t.bytes))))
      .reduce((a, b) => a + b, 0);

    details = (
      <Box>
        <Heading size="md">
          Checkpoint {epoch}
          <Badge marginLeft={2}>{checkpoint.overview?.backend}</Badge>
        </Heading>
        <StatGroup width={718} border="1px solid #666" borderRadius="5px" marginTop={5} padding={3}>
          <Stat>
            <StatLabel>Started</StatLabel>
            <StatNumber>
              {new Intl.DateTimeFormat('en-us', {
                dateStyle: undefined,
                timeStyle: 'medium',
              }).format(new Date(Number(checkpoint.overview?.startTime) / 1000))}
            </StatNumber>
          </Stat>
          <Stat marginLeft={10}>
            <StatLabel>Finished</StatLabel>
            <StatNumber>
              {checkpoint.overview?.finishTime != null
                ? new Intl.DateTimeFormat('en-us', {
                    dateStyle: undefined,
                    timeStyle: 'medium',
                  }).format(new Date(Number(checkpoint.overview?.finishTime) / 1000))
                : '-'}
            </StatNumber>
          </Stat>
          <Stat marginLeft={10}>
            <StatLabel>Duration</StatLabel>
            <StatNumber>{formatDurationHMS(end - start)}</StatNumber>
          </Stat>
          <Stat marginLeft={10}>
            <StatLabel>Size</StatLabel>
            <StatNumber> {util.dataFormat(checkpointBytes)} </StatNumber>
          </Stat>
        </StatGroup>

        <Box marginTop={5}>
          {ops?.map(op => (
            <OperatorCheckpoint key={op.id} op={op} scale={scale} w={w} />
          ))}
        </Box>
      </Box>
    );
  }

  return (
    <Flex>
      <Box w="100px">
        <UnorderedList className="checkpoint-menu">
          {checkpoints
            .slice()
            .reverse()
            .map(c => {
              return (
                <ListItem
                  key={'a' + String(c.epoch)}
                  className={c.epoch == epoch ? 'selected' : ''}
                >
                  <Link onClick={() => setEpoch(c.epoch)}> {c.epoch}</Link>
                </ListItem>
              );
            })}
        </UnorderedList>
      </Box>
      <Box>{details}</Box>
    </Flex>
  );
}
