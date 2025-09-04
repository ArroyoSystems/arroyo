import { Stack } from '@chakra-ui/react';
import { JsonForm } from './JsonForm';
import { Connector, useConnectionProfileAutocomplete } from '../../lib/data_fetching';
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
  const { autocompleteData, autocompleteError } = state.connectionProfileId
    ? useConnectionProfileAutocomplete(state.connectionProfileId)
    : { autocompleteData: undefined, autocompleteError: null };

  return (
    <Stack spacing={8}>
      <Stack spacing="4" maxW={800}>
        <JsonForm
          schema={JSON.parse(connector.table_config)}
          initial={state.table || {}}
          onSubmit={async table => {
            setState({ ...state, table: table });
            onSubmit();
          }}
          error={null}
          button={'Next'}
          autocompleteData={autocompleteData}
          autocompleteError={autocompleteError?.error}
        />
      </Stack>
    </Stack>
  );
};
