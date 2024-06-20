import { Th, Td, Tr, Table, Thead, Tbody } from '@chakra-ui/react';
import { ReactElement } from 'react';
import { OutputData } from '../../lib/data_fetching';

export function PipelineOutputs({ outputs }: { outputs: Array<{ id: number; data: OutputData }> }) {
  let headers: Array<ReactElement> = [];
  const data = outputs
    .map(row => {
      const output = row.data;
      let parsed = JSON.parse(output.value);

      let cols: Array<ReactElement> = [];

      if (headers.length == 0) {
        Object.keys(parsed).forEach(k => {
          headers.push(<Th key={k}>{k}</Th>);
        });
      }

      Object.values(parsed).forEach((v, i) => {
        cols.push(<Td key={i}>{JSON.stringify(v, null, 2)}</Td>);
      });

      return (
        <Tr key={row.id}>
          <Th bgColor={'transparent'} color={'green.400'} key={'row'}>
            {row.id + 1}
          </Th>
          <Th bgColor={'transparent'} color={'green.400'} key={'date'}>
            {new Date(Number(output.timestamp) / 1000).toISOString()}
          </Th>
          {cols}
        </Tr>
      );
    })
    .reverse();

  return (
    <Table
      maxWidth="500px"
      size="sm"
      bgColor="transparent"
      variant={'simple'}
      fontFamily={'roboto-mono,monaco,monospace'}
    >
      <Thead>
        <Tr>
          <Th>Row</Th>
          <Th>Time</Th>
          {headers}
        </Tr>
      </Thead>
      <Tbody>{data}</Tbody>
    </Table>
  );
}
