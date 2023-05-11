import { Box, Text } from '@chakra-ui/react';
import dagre from 'dagre';
import { useMemo } from 'react';
import ReactFlow, { Handle, Position, Background } from 'reactflow';
import { JobNode, JobGraph, JobMetricsResp } from '../../gen/api_pb';
import { getBackpressureColor, getOperatorBackpressure } from '../../lib/util';

function PipelineGraphNode({
  data,
}: {
  data: {
    node: JobNode;
    setActiveOperator: (op: string) => void;
    isActive: boolean;
    operatorBackpressure: number;
  };
}) {
  function handleClick(click: any) {
    data.setActiveOperator(data.node.nodeId);
  }

  let className = 'pipelineGraphNode';
  if (data.isActive) {
    className += ' active';
  }

  return (
    <Box
      bg={getBackpressureColor(data.operatorBackpressure)}
      className={className}
      onClick={handleClick}
    >
      <Handle type="target" position={Position.Top} />
      <Text userSelect="none" pointerEvents="none">
        {data.node.operator}
      </Text>
      <Handle type="source" position={Position.Bottom} />
    </Box>
  );
}

export function PipelineGraph({
  graph,
  metrics,
  setActiveOperator,
  activeOperator,
}: {
  graph: JobGraph;
  metrics?: JobMetricsResp;
  setActiveOperator: (op: string) => void;
  activeOperator?: string;
}) {
  const nodeTypes = useMemo(() => ({ pipelineNode: PipelineGraphNode }), []);

  const nodes = graph.nodes.map(node => {
    return {
      id: node.nodeId,
      type: 'pipelineNode',
      data: {
        label: node.operator,
        node: node,
        setActiveOperator: setActiveOperator,
        isActive: node.nodeId == activeOperator,
        operatorBackpressure: getOperatorBackpressure(metrics, node.nodeId),
      },
      position: {
        x: 0,
        y: 0,
      },
      x: 0,
      y: 0,
      width: 200,
      height: 60,
    };
  });

  const edges = graph.edges.map(edge => {
    return {
      id: `${edge.srcId}-${edge.destId}`,
      source: edge.srcId,
      target: edge.destId,
      type: 'step',
    };
  });

  var g = new dagre.graphlib.Graph();
  g.setGraph({});
  g.setDefaultEdgeLabel(function () {
    return {};
  });

  nodes.forEach(node => g.setNode(node.id, node));
  edges.forEach(edge => g.setEdge(edge.source, edge.target));

  dagre.layout(g);

  nodes.forEach(node => {
    node.position = { x: node.x, y: node.y };
  });

  return (
    <Box className="pipelineGraph">
      <ReactFlow nodes={nodes} edges={edges} nodeTypes={nodeTypes}>
        <Background />
      </ReactFlow>
    </Box>
  );
}
