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
import React, { ChangeEvent, ChangeEventHandler, Dispatch, ReactElement, useState, useMemo } from 'react';
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
import { capitalize } from '../../lib/util';

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
  format: 'json' | 'avro' | 'protobuf';
}) => {
  type SchemaTypeOption = { name: string; value: string };

  const baseSchemaTypeOptions: SchemaTypeOption[] = [
    { name: 'Infer schema', value: 'inferred' },
    { name: capitalize(format) + ' schema', value: 'schema' },
  ];

  const schemaTypeOptions = useMemo(() => {
    let options = [...baseSchemaTypeOptions];

    if (format == 'json') {
      options.push({ name: 'Unstructured JSON', value: 'unstructured' });
    }

    // For Iggy connections, only show Unstructured JSON option
    if (connector.id === 'iggy' && format === 'json') {
      return [{ name: 'Unstructured JSON', value: 'unstructured' }];
    }

    return options;
  }, [connector.id, format]);

  let connectionProfile = null;
  if (state.connectionProfileId != null) {
    connectionProfile = connectionProfiles.find(c => c.id == state.connectionProfileId);
  }

  if (
    connectionProfile != null &&
    ((connector.id == 'kafka' &&
      (connectionProfile.config as any).schemaRegistryEnum?.endpoint != null) ||
      (connector.id == 'confluent' && (connectionProfile.config as any).schemaRegistry != null))
  ) {
    schemaTypeOptions.push({ name: 'Confluent Schema Registry', value: 'confluent' });
  }

  let def_name: 'json_schema' | 'avro_schema' | 'protobuf_schema';
  switch (format) {
    case 'json':
      def_name = 'json_schema';
      break;
    case 'avro':
      def_name = 'avro_schema';
      break;
    case 'protobuf':
      def_name = 'protobuf_schema';
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
  } else if (state.schema?.inferred) {
    editor = (
      <Stack spacing={4} maxW={'lg'}>
        <Text>
          The schema for this connection will be inferred from context in the SQL query. This option
          should generally just be used for sinks.
        </Text>
        <Button onClick={next}>Continue</Button>
      </Stack>
    );
    value = 'inferred';
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
      case 'inferred':
        setState({
          ...state,
          schema: {
            ...state.schema,
            definition: null,
            fields: [],
            format: { json: { unstructured: false, confluentSchemaRegistry: false } },
            inferred: true,
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
                primitive: 'String',
              },
            },
            nullable: false,
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

const RawBytesEditor = ({
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
                primitive: 'Bytes',
              },
            },
            nullable: false,
          },
        ],
        format: { raw_bytes: {} },
      },
    });
    next();
  };

  return (
    <Stack spacing={4} maxW="md">
      <Text>
        When using the raw bytes format, values are read from the source as raw byte strings.
      </Text>

      <Text>
        Raw bytes connection tables have a single <Code>value</Code> column with the value.
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
  const [selectedBadData, setSelectedBadData] = useState<string | undefined>(undefined);

  let { connectionProfiles, connectionProfilesLoading } = useConnectionProfiles();

  if (connectionProfilesLoading) {
    return <></>;
  }

  const allFormats: DataFormatOption[] = [
    {
      name: 'JSON',
      value: 'json',
      el: (
        <SchemaFormatEditor
          key="jsoneditor"
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
          key="avroeditor"
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
      name: 'Raw Bytes',
      value: 'raw_bytes',
      el: <RawBytesEditor state={state} setState={setState} next={next} />,
    },
    {
      name: 'Protobuf',
      value: 'protobuf',
      el: (
        <SchemaFormatEditor
          key="protoeditor"
          connector={connector}
          connectionProfiles={connectionProfiles!}
          state={state}
          setState={setState}
          next={next}
          format={'protobuf'}
        />
      ),
    },
  ];

  // For Iggy connections, only show JSON format option
  const formats = useMemo(() => {
    if (connector.id === 'iggy') {
      return allFormats.filter(format => format.value === 'json');
    }
    return allFormats;
  }, [connector.id, allFormats]);

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

  type BadDataOption = {
    name: string;
    value: components['schemas']['BadData'];
  };

  const badDataOptions: BadDataOption[] = [
    { name: 'Fail', value: { fail: {} } },
    { name: 'Drop', value: { drop: {} } },
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

  const onBadDataChange: ChangeEventHandler<HTMLSelectElement> = e => {
    setSelectedBadData(e.target.value);
    setState({
      ...state,
      schema: {
        ...state.schema,
        fields: [],
        badData: badDataOptions.find(f => f.name == e.target.value)?.value,
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
        <FormLabel>Bad Data</FormLabel>
        <Select
          maxW={'lg'}
          placeholder="Select option"
          value={selectedBadData}
          onChange={onBadDataChange}
        >
          {badDataOptions.map(o => (
            <option key={o.name} value={o.name}>
              {o.name}
            </option>
          ))}
        </Select>
        <FormHelperText maxW={'lg'}>
          This option describes how the job should handle data that doesn't match the defined
          schema. 'Fail' will cause the job to fail, while 'Drop' will cause the job to drop
          (ignore) bad data.
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
