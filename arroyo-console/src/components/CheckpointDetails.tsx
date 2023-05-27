import { Table, TableContainer, Tbody, Td, Th, Thead, Tr, Text } from '@chakra-ui/react';
import {
  CheckpointDetailsResp,
  OperatorCheckpointDetail,
  TaskCheckpointDetail,
} from '../gen/api_pb';
import React from 'react';
import OperatorCheckpointTimeline from './OperatorCheckpointTimeline';
import { dataFormat } from '../lib/util';

export interface CheckpointDetailsProps {
  checkpoint: CheckpointDetailsResp;
  ops:
    | { id: string; name: string; parallelism: number; checkpoint: OperatorCheckpointDetail }[]
    | undefined;
  scale: (x: number) => number;
  w: number;
}

const bytes = (tasks: TaskCheckpointDetail[]) => {
  return tasks.map(t => (t.bytes == null ? 0 : Number(t.bytes))).reduce((a, b) => a + b, 0);
};

const row = (operator: string, timeline: React.ReactElement, totalBytes: number) => {
  return (
    <Tr>
      <Td>
        <Text maxW={400} whiteSpace={'normal'}>
          {operator}
        </Text>
      </Td>
      <Td>{dataFormat(totalBytes)}</Td>
      <Td>{timeline}</Td>
    </Tr>
  );
};

const CheckpointDetails: React.FC<CheckpointDetailsProps> = ({ checkpoint, ops, scale, w }) => {
  const tableBody = (
    <Tbody>
      {ops?.map(op => {
        const size = bytes(Object.values(op.checkpoint.tasks));
        const timeline = <OperatorCheckpointTimeline op={op} scale={scale} w={w} />;
        return row(op.name, timeline, size);
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
            <Th>Timeline</Th>
          </Tr>
        </Thead>
        {tableBody}
      </Table>
    </TableContainer>
  );
};

export default CheckpointDetails;
