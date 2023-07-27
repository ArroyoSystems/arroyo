import { JobMetricsResp } from '../gen/api_pb';
import { Badge, Box, Code, HStack, Spacer } from '@chakra-ui/react';
import { getOperatorBackpressure } from '../lib/util';
import React from 'react';
import { TimeSeriesGraph } from './TimeSeriesGraph';
import Loading from './Loading';
import { PipelineGraph } from '../lib/data_fetching';

export interface OperatorDetailProps {
  operator_id: string;
  graph?: PipelineGraph;
  metrics?: JobMetricsResp;
}

const OperatorDetail: React.FC<OperatorDetailProps> = ({ operator_id, graph, metrics }) => {
  if (graph == undefined || metrics == undefined || metrics.metrics?.[operator_id] == undefined) {
    return <Loading size={'lg'} />;
  } else {
    const node = graph.nodes.find(n => n.nodeId == operator_id);
    const node_metrics = metrics.metrics[operator_id];

    const backpressure = getOperatorBackpressure(metrics, operator_id);

    let backpressureBadge;
    if (backpressure < 0.33) {
      backpressureBadge = <Badge colorScheme={'green'}>LOW</Badge>;
    } else if (backpressure < 0.66) {
      backpressureBadge = <Badge colorScheme={'yellow'}>MEDIUM</Badge>;
    } else {
      backpressureBadge = <Badge colorScheme={'red'}>HIGH</Badge>;
    }

    let msgRecv = 0;
    let msgSent = 0;
    let msgSentData;
    let msgRecvData;

    if (
      node_metrics != null &&
      node_metrics.subtasks != null &&
      node_metrics.subtasks[0].messagesRecv.length > 0
    ) {
      msgRecv = Object.values(node_metrics.subtasks)
        .map(n => n.messagesRecv)
        .reduce((s, a) => s + a[a.length - 1].value, 0);
      msgSent = Object.values(node_metrics.subtasks)
        .map(n => n.messagesSent)
        .reduce((s, a) => s + a[a.length - 1].value, 0);

      msgRecvData = Object.entries(node_metrics.subtasks).map(kv => {
        return kv[1].messagesRecv
          .filter(m => m != undefined)
          .map(m => {
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
  }
};

export default OperatorDetail;
