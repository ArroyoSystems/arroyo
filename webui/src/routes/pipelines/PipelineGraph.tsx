import { Box, Text } from '@chakra-ui/react';
import dagre from 'dagre';
import ReactFlow, { Handle, Position, Background } from 'reactflow';
import { getBackpressureColor, getCurrentMaxMetric } from '../../lib/util';
import { OperatorMetricGroup, PipelineGraph, PipelineNode } from '../../lib/data_fetching';
import { useMemo } from 'react';

function PipelineGraphNode({
  data,
}: {
  data: {
    node: PipelineNode;
    setActiveOperator: (op: number) => void;
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
        {data.node.description}
      </Text>
      <Handle type="source" position={Position.Bottom} />
    </Box>
  );
}

export function PipelineGraphViewer({
  graph,
  operatorMetricGroups,
  setActiveOperator,
  activeOperator,
}: {
  graph: PipelineGraph;
  operatorMetricGroups?: OperatorMetricGroup[];
  setActiveOperator: (node: number) => void;
  activeOperator?: number;
}) {
  const nodeTypes = useMemo(() => ({ pipelineNode: PipelineGraphNode }), []);

  const nodes = graph.nodes.map(node => {
    let backpressure = 0;
    if (operatorMetricGroups && operatorMetricGroups.length > 0) {
      const operatorMetricGroup = operatorMetricGroups.find(o => o.nodeId == node.nodeId);
      if (operatorMetricGroup) {
        const metricGroups = operatorMetricGroup.metricGroups;
        const backpressureMetrics = metricGroups.find(m => m.name == 'backpressure');
        backpressure = backpressureMetrics ? getCurrentMaxMetric(backpressureMetrics) : 0;
      }
    }

    return {
      id: String(node.nodeId),
      type: 'pipelineNode',
      data: {
        label: node.description,
        node: node,
        setActiveOperator: () => {
          console.log(node);
          return setActiveOperator(node.nodeId);
        },
        isActive: node.nodeId == activeOperator,
        operatorBackpressure: backpressure,
      },
      position: {
        x: 0,
        y: 0,
      },
      x: 0,
      y: 0,
      width: 200,
      height: 50,
    };
  });

  const edges = graph.edges.map(edge => {
    return {
      id: `${edge.srcId}-${edge.destId}`,
      source: String(edge.srcId),
      target: String(edge.destId),
      type: 'step',
    };
  });

  var g = new dagre.graphlib.Graph();
  g.setGraph({});
  g.setDefaultEdgeLabel(function () {
    return {};
  });

  nodes.forEach(node => g.setNode(String(node.id), node));
  edges.forEach(edge => g.setEdge(String(edge.source), String(edge.target)));

  dagre.layout(g);

  nodes.forEach(node => {
    node.position = { x: node.x, y: node.y };
  });

  return (
    <Box className="pipelineGraph" h="100%">
      <ReactFlow
        proOptions={{ hideAttribution: true }}
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
      >
        <Background />
      </ReactFlow>
    </Box>
  );
}
