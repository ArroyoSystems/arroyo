import Loading from './Loading';
import { Table, TableContainer, Tbody, Td, Th, Thead, Tr } from '@chakra-ui/react';
import React from 'react';
import { formatDate } from '../lib/util';
import { JobLogMessage } from '../lib/data_fetching';

export interface OperatorErrorsProps {
  operatorErrors?: JobLogMessage[];
}

const OperatorErrors: React.FC<OperatorErrorsProps> = ({ operatorErrors }) => {
  if (!operatorErrors) {
    return <Loading />;
  }

  if (operatorErrors && operatorErrors.length == 0) {
    return <>No errors to display</>;
  }

  const tableBody = (
    <Tbody>
      {operatorErrors.map(m => {
        return (
          <Tr key={String(m.createdAt)}>
            <Td>{formatDate(BigInt(m.createdAt))}</Td>
            <Td>{m.operatorId}</Td>
            <Td>{m.taskIndex?.toString()}</Td>
            <Td>{m.message}</Td>
            <Td>{m.details}</Td>
          </Tr>
        );
      })}
    </Tbody>
  );

  return (
    <TableContainer>
      <Table variant="striped" w={'100%'}>
        <Thead>
          <Tr>
            <Th>Time</Th>
            <Th>Operator</Th>
            <Th>Task Index</Th>
            <Th>Message</Th>
            <Th>Details</Th>
          </Tr>
        </Thead>
        {tableBody}
      </Table>
    </TableContainer>
  );
};

export default OperatorErrors;
