import { Stack, Table, TableContainer, Tbody, Td, Text, Th, Thead, Tr } from '@chakra-ui/react';
import React from 'react';
import { dataFormat } from '../lib/util';
import {
  CheckpointSpanType,
  OperatorCheckpointGroup,
  SubtaskCheckpointGroup,
} from '../lib/data_fetching';

export interface CheckpointDetailsProps {
  operators: OperatorCheckpointGroup[];
}

function formatDuration(micros: number): string {
  let millis = micros / 1000;
  let secs = Math.floor(millis / 1000);
  if (millis < 1000) {
    return `${millis} ms`;
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

const spanDuration = (subtask: SubtaskCheckpointGroup, spanType: CheckpointSpanType) => {
  const span = subtask.eventSpans.find(s => s.spanType == spanType);
  return span ? formatDuration(span.finishTime - span.finishTime) : 'n/a';
};

const spans = (subtasks: SubtaskCheckpointGroup[], spanType: CheckpointSpanType) => {
  return (
    <Stack>
      {subtasks.map(s => (
        <Text>{spanDuration(s, spanType)}</Text>
      ))}
    </Stack>
  );
};

const row = (operator: string, totalBytes: number, subtasks: SubtaskCheckpointGroup[]) => {
  return (
    <Tr>
      <Td>
        <Text maxW={400} whiteSpace={'normal'}>
          {operator}
        </Text>
      </Td>
      <Td>{dataFormat(totalBytes)}</Td>
      <Td>{spans(subtasks, 'alignment')}</Td>
      <Td>{spans(subtasks, 'sync')}</Td>
      <Td>{spans(subtasks, 'async')}</Td>
      <Td>{spans(subtasks, 'committing')}</Td>
    </Tr>
  );
};

const CheckpointDetails: React.FC<CheckpointDetailsProps> = ({ operators }) => {
  const tableBody = (
    <Tbody>
      {operators
        .sort(
          (a, b) => Number(a.operatorId.split('_').pop()) - Number(b.operatorId.split('_').pop())
        )
        .map(op => {
          return row(op.operatorId, op.bytes, op.subtasks);
        })}
    </Tbody>
  );

  return (
    <TableContainer paddingTop={10} paddingBottom={10}>
      <Table size={'sm'} variant="striped">
        <Thead>
          <Tr>
            <Th>Operator</Th>
            <Th>Size</Th>
            <Th>Alignment</Th>
            <Th>Sync</Th>
            <Th>Async</Th>
            <Th>Committing</Th>
          </Tr>
        </Thead>
        {tableBody}
      </Table>
    </TableContainer>
  );
};

export default CheckpointDetails;
