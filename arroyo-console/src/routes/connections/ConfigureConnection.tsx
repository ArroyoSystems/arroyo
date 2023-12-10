import {
  Stack,
} from '@chakra-ui/react';
import { useState } from 'react';
import { JsonForm } from './JsonForm';
import { Connector, useConnectionProfiles } from '../../lib/data_fetching';
import { CreateConnectionState } from './CreateConnection';

export const ConfigureConnection = ({
  connector,
  onSubmit,
  state,
  setState,
}: {
  connector: Connector;
  onSubmit: () => void;
  state: CreateConnectionState;
  setState: (s: CreateConnectionState) => void;
}) => {
  let { connectionProfiles, connectionProfilesLoading, mutateConnectionProfiles } =
    useConnectionProfiles();
  const [clusterError, setClusterError] = useState<string | null>(null);

  if (connectionProfilesLoading) {
    return <></>;
  }

  return (
    <Stack spacing={8}>
      <Stack spacing="4" maxW={800}>
        <JsonForm
          schema={JSON.parse(connector.tableConfig)}
          initial={state.table || {}}
          onSubmit={async table => {
            if (connector?.connectionConfig && state.connectionProfileId == null) {
              setClusterError('Cluster is required');
              return;
            }
            setState({ ...state, table: table });
            onSubmit();
          }}
          error={null}
          button={'Next'}
        />
      </Stack>
    </Stack>
  );
};
