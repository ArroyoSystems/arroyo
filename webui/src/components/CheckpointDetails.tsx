import React, { useEffect, useRef } from 'react';
import { Box, VStack } from '@chakra-ui/react';
import { Checkpoint } from '../lib/data_fetching';
import * as d3 from 'd3';

// Type definitions (assuming these are unchanged)
interface EventSpan {
  spanType: 'alignment' | 'sync' | 'async' | 'committing';
  startTime: number;
  finishTime: number;
}

interface SubtaskCheckpointGroup {
  eventSpans: EventSpan[];
}

interface OperatorCheckpointGroup {
  operatorId: string;
  bytes: number;
  subtasks: SubtaskCheckpointGroup[];
}

interface TimelineData {
  operator: string;
  subtaskIndex: number;
  spanType: string;
  startTime: number;
  finishTime: number;
  duration: number;
  row: number;
  bytes: number;
}

interface CheckpointTimelineProps {
  operators: OperatorCheckpointGroup[];
  checkpoint: Checkpoint;
}

export function formatDuration(micros: number): string {
  let millis = micros / 1000;
  let secs = Math.floor(millis / 1000);
  if (millis < 1000) {
    return `${millis} ms`;
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
    svg.selectAll("*").remove();


    const margin = { top: 30, right: 120, bottom: 50, left: 150 };
    const width = 800 - margin.left - margin.right;
    const height = 250 - margin.top - margin.bottom;

    const tooltip = d3.select(tooltipRef.current!);

    const flatData: TimelineData[] = [];
    let rowIndex = 0;

    const checkpointStartTime = checkpoint.startTime;
    let checkpointEndTime = checkpoint.finishTime;

    operators
      .sort(
        (a, b) => Number(a.operatorId.split('_').pop()) - Number(b.operatorId.split('_').pop())
      )
      .forEach(op => {
        op.subtasks.forEach((subtask, subtaskIndex) => {
          subtask.eventSpans.forEach(span => {
            if (!checkpoint.finishTime) {
              checkpointEndTime = Math.max(checkpointEndTime || 0, span.finishTime);
            }

            flatData.push({
              operator: op.operatorId,
              subtaskIndex,
              spanType: span.spanType,
              startTime: span.startTime - checkpointStartTime,
              finishTime: span.finishTime - checkpointStartTime,
              duration: span.finishTime - span.startTime,
              row: rowIndex,
              bytes: op.bytes
            });
          });
          rowIndex++;
        });
      });

    const checkpointDuration = checkpointEndTime! - checkpointStartTime;
    const xScale = d3.scaleLinear()
      .domain([0, checkpointDuration > 0 ? checkpointDuration : 1])
      .range([0, width]);

    const yScale = d3.scaleBand()
      .domain(d3.range(rowIndex).map(String))
      .range([0, height])
      .padding(0.2);

    const colorScale = d3.scaleOrdinal<string>()
      .domain(['alignment', 'sync', 'async', 'committing'])
      .range(['#ff6b6b', '#4ecdc4', '#45b7d1', '#96ceb4']);

    const svgElement = svg
      .attr("width", width + margin.left + margin.right)
      .attr("height", height + margin.top + margin.bottom);

    svgElement.append("rect")
      .attr("width", "100%")
      .attr("height", "100%")
      .attr("fill", "#1A202C");

    const g = svgElement.append("g")
      .attr("transform", `translate(${margin.left},${margin.top})`);

    const xAxis = d3.axisBottom(xScale)
      .tickFormat(d => formatDuration(d as number));

    const lightTextColor = "#E2E8F0";

    const axisGroup = g.append("g")
      .attr("transform", `translate(0,${height})`)
      .call(xAxis);

    axisGroup.selectAll("text")
      .style("fill", lightTextColor)
      .style("font-size", "11px");
    axisGroup.select(".domain").attr("stroke", lightTextColor);
    axisGroup.selectAll(".tick line").attr("stroke", lightTextColor);

    g.append("text")
      .attr("x", width / 2)
      .attr("y", height + margin.bottom - 10)
      .attr("fill", lightTextColor)
      .style("text-anchor", "middle")
      .style("font-size", "12px")
      .text("Time (relative to checkpoint start)");

    const yLabels: string[] = [];
    operators.forEach(op => {
      op.subtasks.forEach((_, subtaskIndex) => {
        yLabels.push(`${op.operatorId} [${subtaskIndex}]`);
      });
    });

    let currentRow = 0;
    operators.forEach(op => {
      op.subtasks.forEach((subtask, subtaskIndex) => {
        const labelText = subtaskIndex === 0 ? op.operatorId : `[${subtaskIndex}]`;
        const isOperatorLabel = subtaskIndex === 0;

        g.append("text")
          .attr("class", "y-label")
          .attr("x", -15)
          .attr("y", (yScale(String(currentRow)) || 0) + yScale.bandwidth() / 2)
          .attr("dy", "0.35em")
          .style("text-anchor", "end")
          .style("font-size", isOperatorLabel ? "12px" : "11px")
          .style("font-weight", isOperatorLabel ? "bold" : "normal")
          .style("fill", isOperatorLabel ? lightTextColor : "#A0AEC0")
          .text(labelText);

        currentRow++;
      });
    });

    g.selectAll(".timeline-bar")
      .data(flatData)
      .enter()
      .append("rect")
      .attr("class", "timeline-bar")
      .attr("x", d => xScale(d.startTime))
      .attr("y", d => yScale(String(d.row)) || 0)
      .attr("width", d => Math.max(1, xScale(d.finishTime) - xScale(d.startTime)))
      .attr("height", yScale.bandwidth())
      .attr("fill", d => colorScale(d.spanType))
      .attr("opacity", 0.9)
      .attr("stroke", lightTextColor)
      .attr("stroke-width", 0.5);

    g.selectAll('.timeline-bar')
      .on("mouseover", (event: MouseEvent, d: unknown) => {
        const data = d as TimelineData;
        d3.select(event.currentTarget as SVGRectElement).attr("opacity", 1);

        tooltip
          .style("opacity", 1)
          .style("left", `${event.pageX + 10}px`)
          .style("top", `${event.pageY - 10}px`)
          .html(`
            <strong>${data.operator}</strong><br/>
            Subtask: ${data.subtaskIndex}<br/>
            Type: ${data.spanType}<br/>
            Duration: ${formatDuration(data.duration)}<br/>
            Start: ${formatDuration(data.startTime)} (relative)<br/>
            End: ${formatDuration(data.finishTime)} (relative)
          `);
      })
      .on("mouseout", function() {
        d3.select(this).attr("opacity", 0.9);
        tooltip.style("opacity", 0);
      });

    const legend = g.append("g")
      .attr("transform", `translate(${width + 20}, 0)`);

    legend.selectAll(".legend-item")
      .data(colorScale.domain())
      .enter()
      .append("g")
      .attr("class", "legend-item")
      .attr("transform", (d, i) => `translate(0, ${i * 20})`) // Decreased spacing for compact legend
      .each(function(d) {
        const item = d3.select(this);
        item.append("rect")
          .attr("width", 12)
          .attr("height", 12)
          .attr("fill", colorScale(d));
        item.append("text")
          .attr("x", 18)
          .attr("y", 11)
          .style("font-size", "11px") // Adjusted font size
          .style("fill", lightTextColor)
          .text(d);
      });

    g.selectAll(".grid-line")
      .data(xScale.ticks(10))
      .enter()
      .append("line")
      .attr("class", "grid-line")
      .attr("x1", d => xScale(d))
      .attr("x2", d => xScale(d))
      .attr("y1", 0)
      .attr("y2", height)
      .attr("stroke", "#4A5568") // Darker grid lines (example: Chakra gray.600)
      .attr("stroke-width", 0.5);

  }, [operators, checkpoint]);

  return (
    <VStack spacing={4} align="stretch">
      <Box style={{ marginTop: '20px' }}>
        <svg ref={svgRef}></svg>
        <div
          ref={tooltipRef}
          style={{
            position: 'absolute',
            padding: '4px',
            background: 'rgba(26, 32, 44, 0.9)', // Matched to dark theme
            color: 'white',
            borderRadius: '4px',
            fontSize: '12px',
            pointerEvents: 'none',
            opacity: 0,
            transition: 'opacity 0.2s',
            border: '1px solid #E2E8F0'
          }}
        />
      </Box>
    </VStack>
  );
};

export const CheckpointDetails: React.FC<CheckpointTimelineProps> = CheckpointTimeline;