import {
  Alert,
  AlertDescription,
  AlertIcon,
  Badge,
  Box,
  Code,
  HStack,
  Spacer,
} from '@chakra-ui/react';
import { getCurrentMaxMetric, transformMetricGroup } from '../lib/util';
import React from 'react';
import { TimeSeriesGraph } from './TimeSeriesGraph';
import Loading from './Loading';
import { useJobMetrics, usePipeline } from '../lib/data_fetching';
import { components } from '../gen/api-types';

export interface OperatorDetailProps {
  pipelineId: string;
  jobId: string;
  operatorId: string;
}

const OperatorDetail: React.FC<OperatorDetailProps> = ({ pipelineId, jobId, operatorId }) => {
  const { pipeline } = usePipeline(pipelineId);
  const { operatorMetricGroups, operatorMetricGroupsLoading, operatorMetricGroupsError } =
    useJobMetrics(pipelineId, jobId);

  if (operatorMetricGroupsError) {
    return (
      <Alert status="warning">
        <AlertIcon />
        <AlertDescription>Failed to get job's metrics.</AlertDescription>
      </Alert>
    );
  }

  if (!pipeline || !operatorMetricGroups || operatorMetricGroupsLoading) {
    return <Loading size={'lg'} />;
  }

  const node = pipeline.graph.nodes.find(n => n.nodeId == operatorId);
  const operatorMetricGroup = operatorMetricGroups.find(o => o.operatorId == operatorId);

  if (!operatorMetricGroup) {
    return <Loading size={'lg'} />;
  }

  const metricGroups = operatorMetricGroup.metricGroups;

  const backpressureGroup = metricGroups.find(m => m.name == 'backpressure');
  const backpressure = backpressureGroup ? getCurrentMaxMetric(backpressureGroup) : 0;
  let backpressureBadge;
  if (backpressure < 0.33) {
    backpressureBadge = <Badge colorScheme={'green'}>LOW</Badge>;
  } else if (backpressure < 0.66) {
    backpressureBadge = <Badge colorScheme={'yellow'}>MEDIUM</Badge>;
  } else {
    backpressureBadge = <Badge colorScheme={'red'}>HIGH</Badge>;
  }

  function createGraph(
    metricGroups: components['schemas']['MetricGroup'][],
    groupName: string,
    graphTitle: string
  ) {
    let msgCount = 0;
    let graph = <></>;
    const group = metricGroups.find(m => m.name === groupName);
    if (
      group &&
      group.subtasks.length > 0 &&
      group.subtasks.map(s => s.metrics.length).every(l => l > 0)
    ) {
      msgCount = group.subtasks
        .map(s => s.metrics[s.metrics.length - 1].value)
        .reduce((a, c) => a + c, 0);
      const data = transformMetricGroup(group);
      graph = (
        <Box className="chart" marginTop="20px" fontSize={14}>
          {graphTitle}
          <TimeSeriesGraph data={data} timeWindowMs={5 * 60 * 1000} />
        </Box>
      );
    }
    return { msgCount, graph };
  }

  const { msgCount: msgRecv, graph: eventsReceivedGraph } = createGraph(
    metricGroups,
    'messages_recv',
    'Events RX'
  );
  const { msgCount: msgSent, graph: eventsSentGraph } = createGraph(
    metricGroups,
    'messages_sent',
    'Events TX'
  );

  return (
    <Box className="operatorDetail" marginTop={10} padding="10px" border="1px solid #333">
      <HStack fontWeight="semibold">
        <Box>operator</Box>
        <Spacer />
        <Box border="1px solid #aaa" px={1.5} fontSize={12} title="parallelism for this operator">
          {node?.parallelism}
        </Box>
      </HStack>
      <Box marginTop="10px">Backpressure: {backpressureBadge}</Box>
      <Box marginTop="10px">{node?.operator}</Box>
      <Box marginTop="10px" fontFamily="monaco,ubuntu mono,fixed-width">
        <Code>{Math.round(msgRecv)} eps</Code> rx
        <Code marginLeft="20px">{Math.round(msgSent)} eps</Code> tx
      </Box>
      {eventsReceivedGraph}
      {eventsSentGraph}
    </Box>
  );
};

export default OperatorDetail;
