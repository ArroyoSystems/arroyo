import {
  Button,
  Code,
  FormControl,
  FormHelperText,
  FormLabel,
  Link,
  Select,
  Stack,
  Text,
} from '@chakra-ui/react';
import React, { ChangeEvent, ChangeEventHandler, Dispatch, ReactElement, useState } from 'react';
import { CreateConnectionState } from './CreateConnection';
import { SchemaEditor } from './SchemaEditor';
import {
  ConnectionProfile,
  ConnectionSchema,
  Connector,
  useConnectionProfiles,
} from '../../lib/data_fetching';
import { ConfluentSchemaEditor } from './ConfluentSchemaEditor';
import { components } from '../../gen/api-types';

const SchemaFormatEditor = ({
  connector,
  connectionProfiles,
  state,
  setState,
  next,
  format,
}: {
  connector: Connector;
  connectionProfiles: Array<ConnectionProfile>;
  state: CreateConnectionState;
  setState: Dispatch<CreateConnectionState>;
  next: () => void;
  format: 'json' | 'avro';
}) => {
  type SchemaTypeOption = { name: string; value: string };
  let schemaTypeOptions: SchemaTypeOption[] = [{ name: format + ' schema', value: 'schema' }];

  if (format == 'json') {
    schemaTypeOptions.push({ name: 'Unstructured JSON', value: 'unstructured' });
  }

  let connectionProfile = null;
  if (state.connectionProfileId != null) {
    connectionProfile = connectionProfiles.find(c => c.id == state.connectionProfileId);
  }

  if (
    connector.id == 'kafka' &&
    connectionProfile != null &&
    (connectionProfile.config as any).schemaRegistry != null
  ) {
    schemaTypeOptions.push({ name: 'Confluent Schema Registry', value: 'confluent' });
  }

  let def_name: 'json_schema' | 'avro_schema';
  switch (format) {
    case 'json':
      def_name = 'json_schema';
      break;
    case 'avro':
      def_name = 'avro_schema';
      break;
    default:
      throw new Error('unknown format: ' + format);
  }

  let editor;
  let value;
  if ((state.schema?.format![format] || {})['confluentSchemaRegistry']) {
    editor = <ConfluentSchemaEditor state={state} setState={setState} next={next} />;
    value = 'confluent';
  } else if (state.schema?.format?.json?.unstructured) {
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
    value = 'unstructured';
  } else if ((state.schema?.definition || {})[def_name] != undefined) {
    editor = <SchemaEditor state={state} setState={setState} next={next} format={format} />;
    value = 'schema';
  } else {
    editor = null;
    value = undefined;
  }

  const onChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    switch (e.target.value) {
      case 'schema': {
        setState({
          ...state,
          schema: {
            ...state.schema,
            // @ts-ignore
            definition: {
              [def_name]: '',
            },
            fields: [],
            // @ts-ignore
            format: { [format]: {} },
          },
        });
        break;
      }
      case 'confluent':
        setState({
          ...state,
          schema: {
            ...state.schema,
            definition: null,
            fields: [],
            // @ts-ignore
            format: { [format]: { confluentSchemaRegistry: true } },
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
      default:
        setState({
          ...state,
          schema: {
            ...state.schema,
            definition: undefined,
            fields: [],
            // @ts-ignore
            format: { [format]: {} },
          },
        });
    }
  };

  return (
    <Stack spacing={8}>
      <FormControl>
        <FormLabel>Schema type</FormLabel>
        <Select maxW={'lg'} placeholder="Select schema type" value={value} onChange={onChange}>
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
  const [selectedFraming, setSelectedFraming] = useState<string | undefined>(undefined);

  let { connectionProfiles, connectionProfilesLoading } = useConnectionProfiles();

  if (connectionProfilesLoading) {
    return <></>;
  }

  const formats: DataFormatOption[] = [
    {
      name: 'JSON',
      value: 'json',
      el: (
        <SchemaFormatEditor
          connector={connector}
          connectionProfiles={connectionProfiles!}
          state={state}
          setState={setState}
          next={next}
          format={'json'}
        />
      ),
    },
    {
      name: 'Avro',
      value: 'avro',
      el: (
        <SchemaFormatEditor
          connector={connector}
          connectionProfiles={connectionProfiles!}
          state={state}
          setState={setState}
          next={next}
          format={'avro'}
        />
      ),
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
  ];

  type FramingOption = {
    name: string;
    value?: { method: components['schemas']['FramingMethod'] };
    el?: ReactElement;
    disabled?: boolean;
  };
  const framingMethods: FramingOption[] = [
    {
      name: 'None',
    },
    {
      name: 'Newline',
      value: {
        method: {
          newline: {
            maxLineLength: null,
          },
        },
      },
    },
  ];

  const onFormatChange = (e: ChangeEvent<DataFormatOption>) => {
    let format = String(e.target.value);
    setSelectedFormat(format);

    let schema: ConnectionSchema = {
      ...state.schema,
      // @ts-ignore
      format: {},
      fields: [],
      // @ts-ignore
      definition: {},
    };

    // @ts-ignore
    schema.format[format] = {};

    setState({
      ...state,
      schema,
    });
  };

  const onFramingChange: ChangeEventHandler<HTMLSelectElement> = e => {
    setSelectedFraming(e.target.value);
    setState({
      ...state,
      schema: {
        ...state.schema,
        fields: [],
        framing: framingMethods.find(f => f.name == e.target.value)!.value,
      },
    });
  };

  return (
    <Stack spacing={8}>
      <FormControl>
        <FormLabel>Framing</FormLabel>
        <Select maxW={'lg'} value={selectedFraming} onChange={onFramingChange}>
          {framingMethods.map(f => (
            <option key={f.name} value={f.name} disabled={f.disabled}>
              {f.name}
            </option>
          ))}
        </Select>
        <FormHelperText maxW={'lg'}>
          Framing describes how records are framed within the input data. If each input event
          contains a single record this can be left as None.
        </FormHelperText>
      </FormControl>

      <FormControl>
        <FormLabel>Data format</FormLabel>
        <Select
          maxW={'lg'}
          placeholder="Select format"
          value={selectedFormat}
          onChange={onFormatChange}
        >
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
