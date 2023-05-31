import { useOperatorErrors } from '../lib/data_fetching';
import { ApiClient } from '../main';
import Loading from './Loading';
import { Table, TableContainer, Tbody, Td, Th, Thead, Tr } from '@chakra-ui/react';
import React from 'react';
import { formatDate } from '../lib/util';

export interface OperatorErrorsProps {
  client: ApiClient;
  jobId?: string;
}

const OperatorErrors: React.FC<OperatorErrorsProps> = ({ client, jobId }) => {
  const { operatorErrors } = useOperatorErrors(client, jobId);

  if (!operatorErrors) {
    return <Loading />;
  }

  const tableBody = (
    <Tbody>
      {operatorErrors.messages.map(m => {
        return (
          <Tr>
            <Td>{formatDate(m.createdAt)}</Td>
            <Td>{m.operatorId}</Td>
            <Td>{m.taskIndex?.toString()}</Td>
            <Td>{m.message}</Td>
            <Td>{m.details}</Td>
          </Tr>
        );
      })}
    </Tbody>
  );

  const table = (
    <TableContainer width={'100%'} padding={5}>
      <Table variant="striped">
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

  if (operatorErrors && operatorErrors.messages.length == 0) {
    return <>No errors to display</>;
  }

  return <>{table}</>;
};

export default OperatorErrors;
