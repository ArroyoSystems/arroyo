import { Box, Button, Code, FormControl, FormLabel, Link, Select, Stack, StepStatus, Text } from '@chakra-ui/react';
import React, { Dispatch, useState } from 'react';
import { CreateConnectionState } from './CreateConnection';
import { ApiClient } from '../../main';
import { ConnectionSchema, Connector, Format, FormatOptions, JsonSchemaDef } from '../../gen/api_pb';
import { ConfluentSchemaEditor } from './ConfluentSchemaEditor';
import { JsonSchemaEditor } from './JsonSchemaEditor';

const JsonEditor = ({
  connector,
  state,
  setState,
  next,
  client,
}: {
  connector: Connector;
  state: CreateConnectionState;
  setState: Dispatch<CreateConnectionState>;
  next: () => void;
  client: ApiClient;
}) => {
  let editor: JSX.Element | null = null;
  if (state.schema?.definition.case === 'jsonSchema') {
    if (state.schema?.formatOptions?.confluentSchemaRegistry) {
      editor = <ConfluentSchemaEditor
        state={state}
        setState={setState}
        client={client}
        next={next}
      />;
    } else {
      editor = <JsonSchemaEditor state={state} setState={setState} next={next} client={client} />;
    }
  } else if (state.schema?.definition.case === 'rawSchema') {
    editor = (
      <Stack spacing={4} maxW={'lg'}>
        <Text>
          Connection tables configured with an unstructured JSON schema have a single{' '}
          <Code>value</Code> column with the JSON value, which can be accessed using SQL{' '}
          <Link href="https://doc.arroyo.dev/sql/scalar-functions#json-functions">
            JSON functions
          </Link>
          .
        </Text>

        <Button onClick={next}>Continue</Button>
      </Stack>
    );
  };

  const onChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    switch (e.target.value) {
      case 'jsonSchema':
        setState({
          ...state,
          schema: new ConnectionSchema({
            ...state.schema,
            definition: { case: 'jsonSchema', value: '' },
          }),
        });
        break;
      case 'confluentSchemaRegistry':
        setState({
          ...state,
          schema: new ConnectionSchema({
            ...state.schema,
            definition: { case: 'jsonSchema', value: '' },
            formatOptions: new FormatOptions({
              confluentSchemaRegistry: true,
            }),
          }),
        });
        break;
      case 'rawSchema':
        setState({
          ...state,
          schema: new ConnectionSchema({
            ...state.schema,
            definition: { case: 'rawSchema', value: 'value' },
          }),
        });
        break;
    }
  };

  let value;
  switch (state.schema?.definition.case) {
    case 'jsonSchema':
      value = state.schema!.formatOptions?.confluentSchemaRegistry ? 'confluentSchemaRegistry' : 'jsonSchema';
      break;
    case 'rawSchema':
      value = 'rawSchema';
      break;
    default:
      null
  };

  return (
    <Stack spacing={8}>
      <FormControl>
        <FormLabel>Schema type</FormLabel>
        <Select
          maxW={'lg'}
          placeholder="Select schema type"
          value={value}
          onChange={onChange}
        >
          <option value="jsonSchema">Json Schema</option>
          <option value="rawSchema">Unstructured JSON</option>
          {connector.id === 'kafka' && (
            <option value="confluentSchemaRegistry">Confluent Schema Registry</option>
          )}
        </Select>
      </FormControl>

      { editor }
    </Stack>
  );
};

const RawStringEditor = ({
  next,
}: {
  next: () => void;
}) => {
  return (
    <Stack spacing={4} maxW='md'>
      <Text>
        When using the raw string format, values read from the source are interpreted as UTF-8
        encoded strings.
      </Text>

      <Text>
        Raw string connection tables have a single <Code>value</Code> column with the value.
      </Text>

      <Button onClick={next}>Continue</Button>
    </Stack>
  );
}

export const DefineSchema = ({
  connector,
  state,
  setState,
  client,
  next,
}: {
  connector: Connector;
  state: CreateConnectionState;
  setState: Dispatch<CreateConnectionState>;
  client: ApiClient;
  next: () => void;
}) => {
  const formats = [
    {
      name: 'JSON',
      value: Format.JsonFormat,
      el: <JsonEditor connector={connector} state={state} setState={setState} client={client} next={next} />,
    },
    {
      name: 'Raw String',
      value: Format.RawStringFormat,
      el: <RawStringEditor next={next} />,
    },
    {
      name: 'Protobuf (coming soon)',
      value: Format.ProtobufFormat,
      disabled: true,
    },
    {
      name: 'Avro (coming soon)',
      value: Format.AvroFormat,
      disabled: true,
    },
  ];

  return (
    <Stack spacing={8}>
      <FormControl>
        <FormLabel>Data format</FormLabel>
        <Select
          maxW={'lg'}
          placeholder="Select format"
          value={state.schema?.format}
          onChange={e =>
            setState({
              ...state,
              schema: new ConnectionSchema({
                format: parseInt(e.target.value) as Format,
              }),
            })
          }
        >
          {formats.map(f => (
            <option key={f.value} value={f.value} disabled={f.disabled}>
              {f.name}
            </option>
          ))}
        </Select>
      </FormControl>

      {formats.find(f => f.value === state.schema?.format)?.el}
    </Stack>
  );
};
