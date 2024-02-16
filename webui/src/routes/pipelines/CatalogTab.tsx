import React from 'react';
import { Alert, AlertDescription, AlertIcon, Box, Spacer, Stack, Text } from '@chakra-ui/react';
import { Link } from 'react-router-dom';
import { ConnectionTable, useConnectionTables } from '../../lib/data_fetching';
import { Catalog } from './Catalog';

const CatalogTab: React.FC = () => {
  const { connectionTablePages, connectionTablesLoading } = useConnectionTables(50);

  let connectionTables: ConnectionTable[] = [];
  let catalogTruncated = false;
  if (connectionTablePages?.length) {
    connectionTables = connectionTablePages[0].data;
    catalogTruncated = connectionTablePages[0].hasMore;
  }

  const sources = connectionTables.filter(s => s.tableType == 'source');
  const sinks = connectionTables.filter(s => s.tableType == 'sink');

  // Since we only fetch the first page of connection tables,
  // display a warning if there are too many to be shown.
  let catalogTruncatedWarning = <></>;
  if (catalogTruncated) {
    catalogTruncatedWarning = (
      <Alert flexShrink={0} status="warning">
        <AlertIcon />
        <AlertDescription>
          The catalogue is too large to be shown in its entirety. Please see the Connections tab for
          the complete listing.
        </AlertDescription>
      </Alert>
    );
  }

  const catalogType = (name: string, tables: Array<ConnectionTable>) => {
    return (
      <Stack>
        <Text fontSize={'md'} pt={2} pb={4} fontWeight={'bold'}>
          {name.toUpperCase()}S
        </Text>
        <Stack spacing={4}>
          {tables.length == 0 ? (
            <Box overflowY="auto" overflowX="hidden">
              <Text>
                No {name}s have been created. Create one <Link to="/connections/new">here</Link>.
              </Text>
            </Box>
          ) : (
            <Box overflowY="auto" overflowX="hidden">
              <Catalog tables={tables} />
            </Box>
          )}
        </Stack>
      </Stack>
    );
  };

  return (
    <Stack>
      {catalogTruncatedWarning}
      {catalogType('Source', sources)}
      {catalogType('Sink', sinks)}

      <Spacer />
      <Box p={4} borderTop={'1px solid'} borderColor={'gray.500'}>
        Write SQL to create a streaming pipeline. See the{' '}
        <Link to={'http://doc.arroyo.dev/sql'} target="_blank">
          SQL docs
        </Link>{' '}
        for details on Arroyo SQL.
      </Box>
    </Stack>
  );
};

export default CatalogTab;
