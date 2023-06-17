import { Box, Button, FormControl, FormLabel, Select, Stack, StepStatus } from '@chakra-ui/react';
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
    editor = <Box>Raw schema</Box>;
  };

  const onChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    switch (e.target.value) {
      case 'jsonSchema':
        setState({
            ...state,
            schema: new ConnectionSchema({
              ...state.schema,
              definition: { case: "jsonSchema", value: "" }
          }) });
        break;
      case 'confluentSchemaRegistry':
        setState({
            ...state,
            schema: new ConnectionSchema({
              ...state.schema,
              definition: { case: "jsonSchema", value: "" },
              formatOptions: new FormatOptions({
                confluentSchemaRegistry: true,
              })
          }) });
        break;
      case 'unstructured':
        setState({
            ...state,
            schema: new ConnectionSchema({
              ...state.schema,
              definition: { case: "rawSchema", value: "value" }
          }) });
        break;
    }
  };

  return (
    <Stack spacing={8}>
      <FormControl>
        <FormLabel>Schema type</FormLabel>
        <Select
          maxW={'lg'}
          placeholder="Select schema type"
          value={state.schema?.definition.case}
          onChange={onChange}
        >
          <option value="jsonSchema">Json Schema</option>
          <option value="unstructured">Unstructured JSON</option>
          {connector.id === 'kafka' && (
            <option value="confluentSchemaRegistry">Confluent Schema Registry</option>
          )}
        </Select>
      </FormControl>

      { editor }
    </Stack>
  );
};

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
      value: Format.Json,
      el: <JsonEditor connector={connector} state={state} setState={setState} client={client} next={next} />,
    },
    {
      name: 'Raw String',
      value: Format.RawString,
      el: <Box>Raw</Box>,
    },
    {
      name: 'Protobuf (coming soon)',
      value: Format.Protobuf,
      disabled: true,
    },
    {
      name: 'Avro (coming soon)',
      value: Format.Avro,
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
          value={Number(state.schema?.format)}
          onChange={e => setState({ ...state, schema: new ConnectionSchema({
            format: parseInt(e.target.value) as Format
          })})}
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
