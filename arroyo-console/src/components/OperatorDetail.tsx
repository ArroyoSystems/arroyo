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

export interface OperatorDetailProps {
  pipelineId: string;
  jobId: string;
  operatorId: string;
}

const OperatorDetail: React.FC<OperatorDetailProps> = ({ pipelineId, jobId, operatorId }) => {
  const { pipeline } = usePipeline(pipelineId);
  const { operatorMetricGroups, operatorMetricGroupsError } = useJobMetrics(pipelineId, jobId);

  if (operatorMetricGroupsError) {
    return (
      <Alert status="warning">
        <AlertIcon />
        <AlertDescription>Failed to get job's metrics.</AlertDescription>
      </Alert>
    );
  }

  if (!pipeline || !operatorMetricGroups) {
    return <Loading size={'lg'} />;
  }

  const node = pipeline.graph.nodes.find(n => n.nodeId == operatorId);
  const operatorMetricGroup = operatorMetricGroups.find(o => o.operatorId == operatorId);

  if (!operatorMetricGroup) {
    return <Loading size={'lg'} />;
  }

  const metricGroups = operatorMetricGroup.metricGroups;
  const backpressureGroup = metricGroups.find(m => m.name == 'backpressure');
  const messagesRecievedGroup = metricGroups.find(m => m.name == 'messages_recv');
  const messagesSentGroup = metricGroups.find(m => m.name == 'messages_sent');

  if (!backpressureGroup || !messagesRecievedGroup || !messagesSentGroup) {
    return <Loading size={'lg'} />;
  }

  const backpressure = backpressureGroup ? getCurrentMaxMetric(backpressureGroup) : 0;

  let backpressureBadge;
  if (backpressure < 0.33) {
    backpressureBadge = <Badge colorScheme={'green'}>LOW</Badge>;
  } else if (backpressure < 0.66) {
    backpressureBadge = <Badge colorScheme={'yellow'}>MEDIUM</Badge>;
  } else {
    backpressureBadge = <Badge colorScheme={'red'}>HIGH</Badge>;
  }

  const msgRecv = messagesRecievedGroup.subtasks
    .map(s => s.metrics[s.metrics.length - 1].value)
    .reduce((a, c) => a + c, 0);

  const msgSent = messagesSentGroup.subtasks
    .map(s => s.metrics[s.metrics.length - 1].value)
    .reduce((a, c) => a + c, 0);

  const msgSentData = transformMetricGroup(messagesSentGroup);
  const msgRecvData = transformMetricGroup(messagesRecievedGroup);

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
};

export default OperatorDetail;
