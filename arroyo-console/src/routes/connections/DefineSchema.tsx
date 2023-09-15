import { Button, Code, FormControl, FormLabel, Link, Select, Stack, Text } from '@chakra-ui/react';
import React, { ChangeEvent, Dispatch, ReactElement, useState } from 'react';
import { CreateConnectionState } from './CreateConnection';
import { JsonSchemaEditor } from './JsonSchemaEditor';
import { Connector } from '../../lib/data_fetching';
import { ConfluentSchemaEditor } from './ConfluentSchemaEditor';

const JsonEditor = ({
  connector,
  state,
  setState,
  next,
}: {
  connector: Connector;
  state: CreateConnectionState;
  setState: Dispatch<CreateConnectionState>;
  next: () => void;
}) => {
  type SchemaTypeOption = { name: string; value: string };
  let schemaTypeOptions: SchemaTypeOption[] = [
    { name: 'JSON Schema', value: 'json' },
    { name: 'Unstructured JSON', value: 'unstructured' },
  ];
  if (connector.id == 'kafka') {
    schemaTypeOptions.push({ name: 'Confluent Schema Registry', value: 'confluent' });
  }

  const [selectedSchemaType, setSelectedSchemaType] = useState<SchemaTypeOption | undefined>(
    undefined
  );

  let editor: JSX.Element | null = null;
  switch (selectedSchemaType?.value) {
    case 'json':
      editor = <JsonSchemaEditor state={state} setState={setState} next={next} />;
      break;
    case 'confluent':
      editor = <ConfluentSchemaEditor state={state} setState={setState} next={next} />;
      break;
    case 'unstructured':
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
      break;
  }

  const onChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    setSelectedSchemaType(schemaTypeOptions.find(o => o.value == e.target.value));
    switch (e.target.value) {
      case 'json':
        setState({
          ...state,
          schema: {
            ...state.schema,
            definition: { json_schema: '' },
            fields: [],
            format: { json: { unstructured: false, confluentSchemaRegistry: false } },
          },
        });
        break;
      case 'confluent':
        setState({
          ...state,
          schema: {
            ...state.schema,
            definition: { json_schema: '' },
            fields: [],
            format: { json: { unstructured: false, confluentSchemaRegistry: true } },
          },
        });
        break;
      case 'unstructured':
        setState({
          ...state,
          schema: {
            ...state.schema,
            definition: { raw_schema: 'value' },
            fields: [],
            format: { json: { unstructured: true, confluentSchemaRegistry: false } },
          },
        });
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
          value={selectedSchemaType?.value}
          onChange={onChange}
        >
          {schemaTypeOptions.map(s => {
            return (
              <option key={s.value} value={s.value}>
                {s.name}
              </option>
            );
          })}
        </Select>
      </FormControl>

      {editor}
    </Stack>
  );
};

const RawStringEditor = ({
  state,
  setState,
  next,
}: {
  state: CreateConnectionState;
  setState: Dispatch<CreateConnectionState>;
  next: () => void;
}) => {
  const submit = () => {
    setState({
      ...state,
      schema: {
        ...state.schema,
        definition: { raw_schema: 'value' },
        fields: [
          {
            fieldName: 'value',
            fieldType: {
              type: {
                primitive: 'string',
              },
            },
            nullable: true,
          },
        ],
        format: { raw_string: {} },
      },
    });
    next();
  };

  return (
    <Stack spacing={4} maxW="md">
      <Text>
        When using the raw string format, values read from the source are interpreted as UTF-8
        encoded strings.
      </Text>

      <Text>
        Raw string connection tables have a single <Code>value</Code> column with the value.
      </Text>

      <Button onClick={submit}>Continue</Button>
    </Stack>
  );
};

export const DefineSchema = ({
  connector,
  state,
  setState,
  next,
}: {
  connector: Connector;
  state: CreateConnectionState;
  setState: Dispatch<CreateConnectionState>;
  next: () => void;
}) => {
  type DataFormatOption = { name: string; value: string; el?: ReactElement; disabled?: boolean };
  const [selectedFormat, setSelectedFormat] = useState<string | undefined>(undefined);

  const formats: DataFormatOption[] = [
    {
      name: 'JSON',
      value: 'json',
      el: <JsonEditor connector={connector} state={state} setState={setState} next={next} />,
    },
    {
      name: 'Raw String',
      value: 'raw_string',
      el: <RawStringEditor state={state} setState={setState} next={next} />,
    },
    {
      name: 'Protobuf (coming soon)',
      value: 'protobuf',
      disabled: true,
    },
    {
      name: 'Avro (coming soon)',
      value: 'avro',
      disabled: true,
    },
  ];

  const onChange = (e: ChangeEvent<DataFormatOption>) => {
    if (String(e.target.value) == 'json') {
      setSelectedFormat('json');
      setState({
        ...state,
        schema: {
          ...state.schema,
          format: {
            json: { confluentSchemaRegistry: false },
          },
          fields: [],
        },
      });
    } else {
      setSelectedFormat('raw_string');
      setState({
        ...state,
        schema: {
          ...state.schema,
          format: {
            raw_string: {},
          },
          fields: [],
        },
      });
    }
  };

  return (
    <Stack spacing={8}>
      <FormControl>
        <FormLabel>Data format</FormLabel>
        <Select maxW={'lg'} placeholder="Select format" value={selectedFormat} onChange={onChange}>
          {formats.map(f => (
            <option key={f.value} value={f.value} disabled={f.disabled}>
              {f.name}
            </option>
          ))}
        </Select>
      </FormControl>

      {formats.find(f => f.value === selectedFormat)?.el}
    </Stack>
  );
};
