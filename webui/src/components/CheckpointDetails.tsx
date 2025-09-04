import React, { useEffect, useRef } from 'react';
import { Box, VStack } from '@chakra-ui/react';
import { Checkpoint, OperatorCheckpointGroup } from '../lib/data_fetching';
import * as d3 from 'd3';

interface TimelineData {
  operator: string;
  subtaskIndex: number;
  spanType: string;
  start_time: number;
  finish_time: number;
  duration: number;
  row: number;
  bytes: number;
  lane: number;
}

interface YLabelInfo {
  label: string;
  isOperator: boolean;
  isGlobal?: boolean;
}

interface CheckpointTimelineProps {
  operators: OperatorCheckpointGroup[];
  checkpoint: Checkpoint;
}

export function formatDuration(micros: number): string {
  let millis = micros / 1000;
  let secs = Math.floor(millis / 1000);
  if (millis < 1000) {
    return `${millis.toFixed(1)} ms`;
  } else if (millis < 10000) {
    return `${Math.floor(millis)} ms`;
  } else if (secs < 60) {
    return `${secs} s`;
  } else if (millis / 1000 < 60 * 60) {
    let minutes = Math.floor(secs / 60);
    let seconds = secs - minutes * 60;
    return `${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
  } else {
    let hours = Math.floor(secs / (60 * 60));
    let minutes = Math.floor((secs - hours * 60 * 60) / 60);
    let seconds = secs - hours * 60 * 60 - minutes * 60;
    return `${hours}:${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
  }
}

const CheckpointTimeline: React.FC<CheckpointTimelineProps> = ({ operators, checkpoint }) => {
  const svgRef = useRef<SVGSVGElement>(null);
  const tooltipRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!operators.length || !checkpoint) return;

    const svg = d3.select(svgRef.current!);
    svg.selectAll('*').remove();

    const margin = { top: 30, right: 120, bottom: 50, left: 150 };
    const width = 1200;

    const yLabels: YLabelInfo[] = [];
    const initialFlatData: Omit<TimelineData, 'lane'>[] = [];
    let rowIndex = 0;
    const allSpanTypes = new Set<string>();

    const checkpointStartTime = checkpoint.start_time;
    let checkpointEndTime = checkpoint.finish_time;

    if (checkpoint.events && checkpoint.events.length > 0) {
      yLabels.push({ label: 'Global Events', isOperator: true, isGlobal: true });
      const globalRow = rowIndex;
      checkpoint.events.forEach(span => {
        if (!checkpoint.finish_time) {
          checkpointEndTime = Math.max(checkpointEndTime || 0, span.finish_time);
        }
        const spanType = (span as any).event || 'global';
        allSpanTypes.add(spanType);
        initialFlatData.push({
          operator: 'Global',
          subtaskIndex: -2,
          spanType: spanType,
          start_time: span.start_time - checkpointStartTime,
          finish_time: span.finish_time - checkpointStartTime,
          duration: span.finish_time - span.start_time,
          row: globalRow,
          bytes: 0,
        });
      });
      rowIndex++;
    }

    operators
      .sort(
        (a, b) => Number(a.operator_id.split('_').pop()) - Number(b.operator_id.split('_').pop())
      )
      .forEach(op => {
        yLabels.push({ label: op.operator_id, isOperator: true });
        op.subtasks.forEach((_, subtaskIndex) => {
          yLabels.push({ label: `[${subtaskIndex}]`, isOperator: false });
        });

        const operatorRow = rowIndex;
        if (op.started_metadata_write && op.finish_time) {
          if (!checkpoint.finish_time) {
            checkpointEndTime = Math.max(checkpointEndTime || 0, op.finish_time);
          }
          allSpanTypes.add('metadata');
          initialFlatData.push({
            operator: op.operator_id,
            subtaskIndex: -1,
            spanType: 'metadata',
            start_time: op.started_metadata_write - checkpointStartTime,
            finish_time: op.finish_time - checkpointStartTime,
            duration: op.finish_time - op.started_metadata_write,
            row: operatorRow,
            bytes: 0,
          });
        }
        rowIndex++;

        op.subtasks.forEach((subtask, subtaskIndex) => {
          subtask.event_spans.forEach(span => {
            if (!checkpoint.finish_time) {
              checkpointEndTime = Math.max(checkpointEndTime || 0, span.finish_time);
            }
            const spanType = (span as any).event || 'unknown';
            allSpanTypes.add(spanType);
            initialFlatData.push({
              operator: op.operator_id,
              subtaskIndex,
              spanType: spanType,
              start_time: span.start_time - checkpointStartTime,
              finish_time: span.finish_time - checkpointStartTime,
              duration: span.finish_time - span.start_time,
              row: rowIndex,
              bytes: op.bytes,
            });
          });
          rowIndex++;
        });
      });

    const layoutData: TimelineData[] = [];
    const rowLaneCounts = new Map<number, number>();
    const groupedByRow = d3.group(initialFlatData, d => d.row);

    for (const [row, spans] of groupedByRow.entries()) {
      spans.sort((a, b) => a.start_time - b.start_time);
      const lanes: number[] = [];
      for (const span of spans) {
        let placed = false;
        for (let i = 0; i < lanes.length; i++) {
          if (span.start_time >= lanes[i]) {
            lanes[i] = span.finish_time;
            layoutData.push({ ...span, lane: i });
            placed = true;
            break;
          }
        }
        if (!placed) {
          lanes.push(span.finish_time);
          const newLaneIndex = lanes.length - 1;
          layoutData.push({ ...span, lane: newLaneIndex });
        }
      }
      rowLaneCounts.set(row, lanes.length || 1);
    }

    const laneHeight = 18;
    const rowPadding = 8;
    const yPositions = new Map<number, { y: number; height: number }>();
    let currentY = 0;

    for (let i = 0; i < yLabels.length; i++) {
      const numLanes = rowLaneCounts.get(i) || 1;
      const totalRowHeight = numLanes * laneHeight;
      yPositions.set(i, { y: currentY, height: totalRowHeight });
      currentY += totalRowHeight + rowPadding;
    }
    const height = currentY;

    const tooltip = d3.select(tooltipRef.current!);
    const checkpointDuration = checkpointEndTime! - checkpointStartTime;
    const xScale = d3
      .scaleLinear()
      .domain([0, checkpointDuration > 0 ? checkpointDuration : 1])
      .range([0, width]);

    // Dynamic color scale
    const colorScale = d3.scaleOrdinal<string>(d3.schemeTableau10).domain(Array.from(allSpanTypes));

    const svgElement = svg
      .attr('width', width + margin.left + margin.right)
      .attr('height', height + margin.top + margin.bottom);

    svgElement.append('rect').attr('width', '100%').attr('height', '100%').attr('fill', '#1A202C');

    const g = svgElement.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

    const xAxis = d3.axisBottom(xScale).tickFormat(d => formatDuration(d as number));

    const lightTextColor = '#E2E8F0';

    const axisGroup = g.append('g').attr('transform', `translate(0,${height})`).call(xAxis);

    g.append('text')
      .attr('x', width / 2)
      .attr('y', height + margin.bottom - 10)
      .attr('fill', lightTextColor)
      .style('text-anchor', 'middle')
      .style('font-size', '12px')
      .text('Time (relative to checkpoint start)');

    g.selectAll('.y-label')
      .data(yLabels)
      .enter()
      .append('text')
      .attr('class', 'y-label')
      .attr('x', -15)
      .attr('y', (d, i) => (yPositions.get(i)?.y || 0) + (yPositions.get(i)?.height || 0) / 2)
      .attr('dy', '0.35em')
      .style('text-anchor', 'end')
      .style('font-size', d => (d.isOperator ? '12px' : '11px'))
      .style('stroke', lightTextColor)
      .text(d => d.label);

    g.selectAll('.timeline-bar')
      .data(layoutData)
      .enter()
      .append('rect')
      .attr('class', 'timeline-bar')
      .attr('x', d => xScale(d.start_time))
      .attr('y', d => (yPositions.get(d.row)?.y || 0) + d.lane * laneHeight)
      .attr('width', d => Math.max(1, xScale(d.finish_time) - xScale(d.start_time)))
      .attr('height', laneHeight - 2)
      .attr('fill', d => colorScale(d.spanType))
      .attr('opacity', 0.9)
      .attr('stroke', lightTextColor)
      .attr('stroke-width', 0.5);

    g.selectAll('.timeline-bar')
      .on('mouseover', (event: MouseEvent, d: unknown) => {
        const data = d as TimelineData;
        d3.select(event.currentTarget as SVGRectElement).attr('opacity', 1);

        let tooltipHtml = '';
        if (data.subtaskIndex === -2) {
          tooltipHtml = `
              <strong>Global Event</strong><br/>
              Type: ${data.spanType}<br/>
              Duration: ${formatDuration(data.duration)}
            `;
        } else if (data.subtaskIndex === -1) {
          tooltipHtml = `
              <strong>${data.operator}</strong><br/>
              Type: Metadata Write<br/>
              Duration: ${formatDuration(data.duration)}
            `;
        } else {
          // Subtask span
          tooltipHtml = `
              <strong>${data.operator}</strong><br/>
              Subtask: ${data.subtaskIndex}<br/>
              Type: ${data.spanType}<br/>
              Duration: ${formatDuration(data.duration)}
            `;
        }
        tooltip
          .style('opacity', 1)
          .style('left', `${event.clientX + 10}px`)
          .style('top', `${event.clientY - 10}px`)
          .html(tooltipHtml);
      })
      .on('mouseout', function () {
        d3.select(this).attr('opacity', 0.9);
        tooltip.style('opacity', 0);
      });

    g.selectAll('.grid-line').attr('y2', height);
  }, [operators, checkpoint]);

  return (
    <VStack spacing={4} align="stretch">
      <Box style={{ marginTop: '20px' }} overflowX="auto">
        <svg ref={svgRef}></svg>
        <div
          ref={tooltipRef}
          style={{
            position: 'fixed',
            padding: '8px',
            background: 'rgba(26, 32, 44, 0.9)',
            color: 'white',
            borderRadius: '4px',
            fontSize: '12px',
            pointerEvents: 'none',
            opacity: 0,
            transition: 'opacity 0.2s',
            border: '1px solid #E2E8F0',
            maxWidth: '300px',
          }}
        />
      </Box>
    </VStack>
  );
};

export const CheckpointDetails: React.FC<CheckpointTimelineProps> = CheckpointTimeline;
