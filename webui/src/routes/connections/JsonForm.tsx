import {
  Alert,
  AlertIcon,
  Box,
  Button,
  Flex,
  FormControl,
  FormErrorMessage,
  FormHelperText,
  FormLabel,
  HStack,
  IconButton,
  Input,
  ListItem,
  Select,
  Spinner,
  Stack,
  Switch,
  Text,
  Textarea,
  Tooltip,
  UnorderedList,
} from '@chakra-ui/react';
import { JSONSchema7 } from 'json-schema';
import { useFormik } from 'formik';

import Ajv from 'ajv/dist/2019';
import addFormats from 'ajv-formats';
import React, { useEffect, useMemo } from 'react';
import { AddIcon, DeleteIcon, WarningIcon } from '@chakra-ui/icons';
import Markdown from 'react-markdown';
import { useCombobox } from 'downshift';

const CustomMarkdown = (props: any) => (
  <Markdown
    components={{
      a({ node, children, ...props }) {
        let url = new URL(props.href ?? '', location.href);
        if (url.origin !== location.origin) {
          props.target = '_blank';
          props.rel = 'noopener noreferrer';
        }
        return <a {...props}>{children}</a>;
      },
    }}
  >
    {props.children}
  </Markdown>
);

function StringWidget({
  path,
  title,
  description,
  placeholder,
  format,
  required,
  password,
  maxLength,
  value,
  errors,
  onChange,
  readonly,
  resetField,
}: {
  path: string;
  title: string;
  description?: string;
  placeholder?: string;
  format?: string;
  maxLength?: number;
  required?: boolean;
  password?: boolean;
  value: string;
  errors: any;
  onChange: (e: React.ChangeEvent<any>) => void;
  readonly?: boolean;
  resetField?: (field: string) => any;
}) {
  const onChangeWrapper = (e: React.ChangeEvent<any>) => {
    onChange(e);

    if (resetField && e.target.value == '') {
      resetField(path);
    }
  };

  return (
    <FormControl isRequired={required} isInvalid={errors[path]}>
      <FormLabel>{title}</FormLabel>
      {maxLength == null || maxLength < 100 ? (
        <Input
          name={path}
          type={password ? 'password' : 'text'}
          placeholder={readonly ? undefined : placeholder}
          value={value || ''}
          onChange={onChangeWrapper}
          readOnly={readonly}
        />
      ) : (
        <Textarea
          name={path}
          placeholder={placeholder}
          value={value || ''}
          onChange={onChangeWrapper}
          resize={'vertical'}
          size={'md'}
          readOnly={readonly}
        />
      )}
      {errors[path] ? (
        <FormErrorMessage>{errors[path]}</FormErrorMessage>
      ) : (
        description && (
          <FormHelperText>
            <CustomMarkdown>{description}</CustomMarkdown>
            {format == 'var-str' && (
              <Text mt={1} fontSize="sm" color="gray.500">
                This field supports{' '}
                <dfn title="To use variable substitution, wrap your variable in double-braces like `{{ MY_VAR }}`">
                  enviroment variable substitution
                </dfn>
                .
              </Text>
            )}
          </FormHelperText>
        )
      )}
    </FormControl>
  );
}

function NumberWidget({
  path,
  title,
  description,
  placeholder,
  required,
  type,
  min,
  max,
  value,
  errors,
  onChange,
  readonly,
}: {
  path: string;
  title: string;
  description?: string;
  placeholder?: number;
  required?: boolean;
  type: 'number' | 'integer';
  min?: number;
  max?: number;
  value: number;
  errors: any;
  onChange: (e: React.ChangeEvent<any>) => void;
  readonly?: boolean;
}) {
  return (
    <FormControl isRequired={required} isInvalid={errors[path]}>
      <FormLabel>{title}</FormLabel>
      <Input
        name={path}
        type="number"
        step={type === 'integer' ? 1 : undefined}
        min={min}
        max={max}
        placeholder={placeholder ? String(placeholder) : undefined}
        value={value || ''}
        onChange={e => onChange(e)}
        readOnly={readonly}
      />
      {errors[path] ? (
        <FormErrorMessage>{errors[path]}</FormErrorMessage>
      ) : (
        description && (
          <FormHelperText>
            <CustomMarkdown>{description}</CustomMarkdown>
          </FormHelperText>
        )
      )}
    </FormControl>
  );
}

function BooleanWidget({
  path,
  title,
  description,
  value,
  errors,
  onChange,
  readonly,
}: {
  path: string;
  title: string;
  description?: string;
  value: boolean;
  errors: any;
  onChange: (e: React.ChangeEvent<any>) => void;
  readonly?: boolean;
}) {
  useEffect(() => {
    if (!value) {
      // @ts-ignore
      onChange({ target: { name: path, value: false } });
    }
  }, [path]);

  return (
    <FormControl isInvalid={errors[path]}>
      <HStack alignItems={'middle'}>
        <FormLabel>{title}</FormLabel>
        <Switch name={path} isChecked={value} readOnly={readonly} onChange={e => onChange(e)} />
      </HStack>
      {errors[path] ? (
        <FormErrorMessage>{errors[path]}</FormErrorMessage>
      ) : (
        description && (
          <FormHelperText>
            <CustomMarkdown>{description}</CustomMarkdown>
          </FormHelperText>
        )
      )}
    </FormControl>
  );
}

function AutocompleteWidget({
  path,
  title,
  description,
  placeholder,
  required,
  value,
  errors,
  onChange,
  readonly,
  autocompleteData,
  autocompleteError,
}: {
  path: string;
  title: string;
  description?: string;
  placeholder?: string;
  required?: boolean;
  value: string;
  errors: any;
  onChange: (e: React.ChangeEvent<any>) => void;
  readonly?: boolean;
  autocompleteData?: {
    values: {
      [key: string]: string[] | undefined;
    };
  };
  autocompleteError?: string;
}) {
  const ourItems = autocompleteData?.values[path] || [];
  const [allItems, setAllItems] = React.useState<string[]>(ourItems);
  const [items, setItems] = React.useState<string[]>(ourItems);

  useEffect(() => {
    if (autocompleteData && allItems.length == 0) {
      setAllItems(ourItems);
      setItems(ourItems);
    }
  }, [autocompleteData]);

  const {
    isOpen,
    getToggleButtonProps,
    getLabelProps,
    getMenuProps,
    getInputProps,
    highlightedIndex,
    getItemProps,
  } = useCombobox({
    onInputValueChange({ inputValue }) {
      // @ts-ignore
      onChange({ target: { name: path, value: inputValue } });
      if (inputValue && inputValue?.length > 0) {
        setItems(allItems.filter(items => items.toLowerCase().includes(inputValue.toLowerCase())));
      } else {
        setItems(allItems);
      }
    },
    initialInputValue: value,
    items,
  });

  return (
    <FormControl isRequired={required} isInvalid={errors[path]}>
      <FormLabel {...getLabelProps()}>{title}</FormLabel>
      <HStack>
        <Input
          name={path}
          type={'text'}
          placeholder={placeholder}
          onChange={e => onChange(e)}
          readOnly={readonly}
          {...getInputProps()}
        />
        <Tooltip
          label={
            autocompleteError
              ? autocompleteError
              : autocompleteData
              ? 'Show options'
              : 'Data loading...'
          }
        >
          <Button
            {...getToggleButtonProps()}
            isDisabled={autocompleteError != undefined || allItems.length == 0}
            size={'sm'}
          >
            {autocompleteError ? (
              <WarningIcon />
            ) : autocompleteData ? (
              <Text p={1}>▼</Text>
            ) : (
              <Spinner size={'sm'} speed={'0.5s'} />
            )}
          </Button>
        </Tooltip>
        <UnorderedList
          position={'absolute'}
          top={20}
          left={-3}
          {...getMenuProps()}
          bg={'gray.700'}
          zIndex={100}
          borderColor={'gray.500'}
          borderWidth={'1px'}
          borderRadius={8}
          listStyleType={'none'}
          maxH={'400px'}
          w={'90%'}
          display={isOpen && items.length > 0 ? 'block' : 'none'}
          opacity={0.95}
          overflowY={'auto'}
        >
          {isOpen &&
            items.map((item, index) => (
              <ListItem
                cursor={'default'}
                p={2}
                m={1}
                key={`${item}${index}`}
                {...getItemProps({ item, index })}
                bg={highlightedIndex === index ? 'blue.600' : undefined}
                borderRadius={4}
              >
                {item}
              </ListItem>
            ))}
        </UnorderedList>
      </HStack>

      {errors[path] ? (
        <FormErrorMessage>{errors[path]}</FormErrorMessage>
      ) : (
        description && (
          <FormHelperText>
            <CustomMarkdown>{description}</CustomMarkdown>
          </FormHelperText>
        )
      )}
    </FormControl>
  );
}

function SelectWidget({
  path,
  valuePath,
  title,
  description,
  placeholder,
  options,
  value,
  onChange,
  defaultValue,
  resetField,
  readonly,
}: {
  path: string;
  valuePath: string;
  title?: string;
  description?: string;
  placeholder?: string;
  options: Array<{ value: string; label: string }>;
  value: string;
  onChange: (e: React.ChangeEvent<any>) => void;
  defaultValue?: string;
  resetField: (field: string) => any;
  readonly?: boolean;
}) {
  useEffect(() => {
    if (!value) {
      if (defaultValue) {
        // @ts-ignore
        onChange({ target: { name: path, value: defaultValue } });
      } else {
        // @ts-ignore
        onChange({ target: { name: path, value: options[0].value } });
      }
    }
  });

  const onChangeWrapper = (e: React.ChangeEvent<any>) => {
    resetField(valuePath);
    onChange(e);
  };

  return (
    <FormControl>
      {title && <FormLabel>{title}</FormLabel>}
      <Select
        placeholder={placeholder}
        name={path}
        value={value}
        onChange={onChangeWrapper}
        borderColor={'gray.600'}
        isReadOnly={readonly}
        isDisabled={readonly}
      >
        {options.map(option => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </Select>
      {description && (
        <FormHelperText>
          <CustomMarkdown>{description}</CustomMarkdown>
        </FormHelperText>
      )}
    </FormControl>
  );
}

export function ArrayWidget({
  schema,
  onChange,
  path,
  values,
  errors,
  readonly,
}: {
  schema: JSONSchema7;
  onChange: (e: React.ChangeEvent<any>) => void;
  path: string;
  values: any;
  errors: any;
  readonly?: boolean;
}) {
  const add = () => {
    // @ts-ignore
    onChange({ target: { name: path, value: [...(values || []), undefined] } });
  };

  const deleteItem = (index: number) => {
    values.splice(index, 1);
    // @ts-ignore
    onChange({ target: { name: path, value: values } });
  };

  const itemsSchema = schema.items as JSONSchema7;

  const example =
    itemsSchema.examples && Array.isArray(itemsSchema.examples)
      ? (itemsSchema.examples[0] as string)
      : undefined;

  const arrayItem = (v: string, i: number) => {
    switch (itemsSchema.type) {
      case 'string':
        return (
          <StringWidget
            path={`${path}.${i}`}
            title={itemsSchema.title + ` ${i + 1}`}
            value={v}
            errors={errors}
            onChange={onChange}
            maxLength={itemsSchema.maxLength}
            description={itemsSchema.description}
            placeholder={example}
            format={itemsSchema.format}
            readonly={readonly}
          />
        );
      default:
        console.warn('Unsupported array item type', itemsSchema.type);
        return <></>;
    }
  };

  return (
    <Box>
      <fieldset key={schema.title} style={{ border: '1px solid #888', borderRadius: '8px' }}>
        <legend
          style={{
            marginLeft: '8px',
            paddingLeft: '16px',
            paddingRight: '16px',
          }}
        >
          {schema.title}
        </legend>
        <FormControl isInvalid={errors[path]}>
          <Stack p={4} gap={2}>
            {errors[path] ? (
              <FormErrorMessage>{errors[path]}</FormErrorMessage>
            ) : (
              schema.description && (
                <FormHelperText mt={0} pb={2}>
                  {schema.description}
                </FormHelperText>
              )
            )}
            {values?.map((value: any, index: number) => (
              <Flex alignItems={'flex-end'} gap={2} key={index}>
                {arrayItem(value, index)}
                {!readonly && (
                  <IconButton
                    width={8}
                    height={8}
                    minWidth={0}
                    aria-label="Delete item"
                    onClick={() => deleteItem(index)}
                    icon={<DeleteIcon width={3} />}
                  />
                )}
              </Flex>
            ))}
            {!readonly && (
              <IconButton
                mt={1}
                height={8}
                aria-label="Add item"
                onClick={add}
                icon={<AddIcon />}
              />
            )}
          </Stack>
        </FormControl>
      </fieldset>
    </Box>
  );
}

export function MapWidget({
  schema,
  onChange,
  path,
  values,
  errors,
  readonly,
}: {
  schema: JSONSchema7;
  onChange: (e: React.ChangeEvent<any>) => void;
  path: string;
  values: any;
  errors: any;
  readonly?: boolean;
}) {
  interface KeyValuePair {
    key: string;
    value: any;
    error: string | undefined;
  }

  const initial = values ? Object.entries(values).map(([key, value]) => ({ key, value })) : [];
  // @ts-ignore
  const [keyValues, setKeyValues] = React.useState<KeyValuePair[]>(initial);

  const itemsSchema = schema.additionalProperties as JSONSchema7;

  if (itemsSchema.type != 'string') {
    console.warn('Unsupported map item type', itemsSchema.type);
    return <></>;
  }

  const saveKeyValues = (keyValuePairs: KeyValuePair[]) => {
    let newValues = {};
    keyValuePairs.forEach(pair => {
      if (pair.key) {
        pair.error = undefined;
        if (pair.key in newValues) {
          pair.error = 'Must have unique key';
          setKeyValues(keyValuePairs);
        } else {
          // @ts-ignore
          newValues[pair.key] = pair.value;
        }
      }
    });
    // @ts-ignore
    onChange({ target: { name: path, value: newValues } });
  };

  const deleteItem = (index: number) => {
    const newKeyValues = keyValues.filter((pair, i) => i != index);
    setKeyValues(newKeyValues);
    saveKeyValues(newKeyValues);
  };

  const add = () => {
    const newPair: KeyValuePair = { key: '', value: '', error: undefined };
    const newKeyValues = [...keyValues, newPair];
    setKeyValues(newKeyValues);
    saveKeyValues(newKeyValues);
  };

  const updateKeyValue = (i: number, key: string, value: string, error?: string) => {
    const newKeyValues = keyValues.map((pair, index) => {
      if (index == i) {
        return { key, value, error };
      } else {
        return pair;
      }
    });
    setKeyValues(newKeyValues);
    saveKeyValues(newKeyValues);
  };

  const mapItem = (pair: KeyValuePair, i: number) => {
    return (
      <FormControl isRequired={true} isInvalid={pair.error != undefined}>
        <Flex gap={2} alignItems={'flex-end'}>
          <Flex gap={2} flex={1} alignItems={'center'} key={i}>
            <Input
              id={i + '_key'}
              value={pair.key}
              onChange={e => updateKeyValue(i, e.target.value, pair.value)}
              readOnly={readonly}
            />
            <Text>→</Text>
            <Input
              id={i + '_value'}
              value={pair.value}
              onChange={e => updateKeyValue(i, pair.key, e.target.value)}
              readOnly={readonly}
            />
          </Flex>
          {!readonly && (
            <IconButton
              width={8}
              height={8}
              minWidth={0}
              aria-label="Delete item"
              onClick={() => deleteItem(i)}
              icon={<DeleteIcon width={3} />}
            />
          )}
        </Flex>
        <FormErrorMessage>{pair.error}</FormErrorMessage>
      </FormControl>
    );
  };

  return (
    <Box>
      <fieldset key={schema.title} style={{ border: '1px solid #888', borderRadius: '8px' }}>
        <legend
          style={{
            marginLeft: '8px',
            paddingLeft: '16px',
            paddingRight: '16px',
          }}
        >
          {schema.title}
        </legend>
        <FormControl isInvalid={errors[path]}>
          <Stack p={4} gap={2}>
            {errors[path] ? (
              <FormErrorMessage>{errors[path]}</FormErrorMessage>
            ) : (
              schema.description && (
                <FormHelperText mt={0} pb={2}>
                  <CustomMarkdown>{schema.description}</CustomMarkdown>
                </FormHelperText>
              )
            )}
            {keyValues.map((pair, index) => mapItem(pair, index))}
            {!readonly && (
              <IconButton
                mt={1}
                height={8}
                aria-label="Add item"
                onClick={add}
                icon={<AddIcon />}
              />
            )}
          </Stack>
        </FormControl>
      </fieldset>
    </Box>
  );
}

export function FormInner({
  schema,
  onChange,
  path,
  values,
  errors,
  resetField,
  readonly,
  autocompleteData,
  autocompleteError,
}: {
  schema: JSONSchema7;
  onChange: (e: React.ChangeEvent<any>) => void;
  path?: string;
  values: any;
  errors: any;
  resetField: (field: string) => void;
  readonly?: boolean;
  autocompleteData?: {
    values: {
      [key: string]: string[] | undefined;
    };
  };
  autocompleteError?: string;
}) {
  useEffect(() => {
    if (!schema.properties || Object.keys(schema.properties).length == 0) {
      // @ts-ignore
      onChange({ target: { name: path, value: {} } });
    }
  }, []);

  function traversePath(values: any, typeKey: string): any {
    let value = values;
    typeKey.split('.').forEach(key => {
      value = value && value[key];
    });
    return value;
  }

  return (
    <Stack spacing={6}>
      {Object.keys(schema.properties || {})
        .filter(key => {
          const property = schema.properties![key];
          // @ts-ignore
          return !property.deprecated;
        })
        .map(key => {
          const property = schema.properties![key];
          const nextPath = (path ? `${path}.` : '') + key;
          if (typeof property == 'object') {
            switch (property.type) {
              case 'string':
                if (property.enum) {
                  return (
                    <SelectWidget
                      path={nextPath}
                      valuePath={nextPath}
                      key={key}
                      title={property.title || key}
                      description={property.description}
                      options={property.enum.map(value => ({
                        value: value!.toString(),
                        label: value!.toString(),
                      }))}
                      value={traversePath(values, nextPath)}
                      onChange={onChange}
                      defaultValue={property.default?.toString()}
                      resetField={resetField}
                      readonly={readonly}
                    />
                  );
                } else if (property.format == 'autocomplete') {
                  return (
                    <AutocompleteWidget
                      path={nextPath}
                      key={key}
                      title={property.title || key}
                      value={traversePath(values, nextPath)}
                      errors={errors}
                      onChange={onChange}
                      readonly={readonly}
                      autocompleteData={autocompleteData}
                      autocompleteError={autocompleteError}
                    />
                  );
                } else {
                  return (
                    <StringWidget
                      path={nextPath}
                      key={key}
                      title={property.title || key}
                      description={property.description}
                      required={schema.required?.includes(key)}
                      // @ts-ignore
                      password={schema.sensitive?.includes(key)}
                      maxLength={property.maxLength}
                      // @ts-ignore
                      placeholder={property.examples ? (property.examples[0] as string) : undefined}
                      format={property.format}
                      value={traversePath(values, nextPath)}
                      errors={errors}
                      onChange={onChange}
                      readonly={readonly}
                      resetField={resetField}
                    />
                  );
                }
              case 'number':
              case 'integer': {
                return (
                  <NumberWidget
                    path={nextPath}
                    key={key}
                    title={property.title || key}
                    description={property.description}
                    required={schema.required?.includes(key)}
                    type={property.type}
                    placeholder={
                      // @ts-ignore
                      property.examples ? (property.examples[0] as number) : undefined
                    }
                    min={property.minimum}
                    max={property.maximum}
                    value={traversePath(values, nextPath)}
                    errors={errors}
                    onChange={onChange}
                    readonly={readonly}
                  />
                );
              }
              case 'boolean': {
                return (
                  <BooleanWidget
                    path={nextPath}
                    title={property.title || key}
                    description={property.description}
                    value={traversePath(values, nextPath)}
                    errors={errors}
                    onChange={onChange}
                    readonly={readonly}
                  />
                );
              }
              case 'array': {
                return (
                  <ArrayWidget
                    path={nextPath}
                    key={key}
                    schema={property}
                    values={traversePath(values, nextPath)}
                    errors={errors}
                    onChange={onChange}
                    readonly={readonly}
                  />
                );
              }
              case 'object': {
                if (property.oneOf) {
                  const typeKey = '__meta.' + nextPath + '.type';
                  let value = traversePath(values, typeKey);

                  // @ts-ignore
                  const inSchema = property.oneOf.find(x => x.title == value) || property.oneOf[0];
                  return (
                    <fieldset key={key} style={{ border: '1px solid #888', borderRadius: '8px' }}>
                      <legend
                        style={{ marginLeft: '8px', paddingLeft: '16px', paddingRight: '16px' }}
                      >
                        {property.title || key}
                      </legend>
                      <Stack p={4}>
                        <SelectWidget
                          path={typeKey}
                          valuePath={nextPath}
                          description={property.description}
                          options={property.oneOf.map(oneOf => ({
                            // @ts-ignore
                            value: oneOf.title!,
                            // @ts-ignore
                            label:
                              // @ts-ignore
                              oneOf.title! +
                              // @ts-ignore
                              (oneOf.description ? ` — ${oneOf.description.toLowerCase()}` : ''),
                          }))}
                          value={value}
                          onChange={onChange}
                          resetField={resetField}
                          readonly={readonly}
                        />
                        {value != undefined && (
                          <Box p={4}>
                            <FormInner
                              path={nextPath}
                              key={value}
                              // @ts-ignore
                              schema={inSchema}
                              errors={errors}
                              onChange={onChange}
                              values={values}
                              resetField={resetField}
                              readonly={readonly}
                            />
                          </Box>
                        )}
                      </Stack>
                    </fieldset>
                  );
                } else if (property.properties != undefined) {
                  return (
                    <FormControl isInvalid={errors[nextPath]}>
                      <fieldset key={key} style={{ border: '1px solid #888', borderRadius: '8px' }}>
                        <legend
                          style={{ marginLeft: '8px', paddingLeft: '16px', paddingRight: '16px' }}
                        >
                          {property.title || key}
                        </legend>
                        <Box p={4}>
                          <FormInner
                            path={nextPath}
                            // @ts-ignore
                            schema={property}
                            errors={errors}
                            onChange={onChange}
                            values={values}
                            resetField={resetField}
                            readonly={readonly}
                          />
                        </Box>
                      </fieldset>
                      <FormErrorMessage>{errors[nextPath] ?? ''}</FormErrorMessage>
                    </FormControl>
                  );
                } else if (property.additionalProperties) {
                  return (
                    <Box key={key}>
                      <MapWidget
                        path={nextPath}
                        key={key}
                        schema={property}
                        values={traversePath(values, nextPath)}
                        errors={errors}
                        onChange={onChange}
                        readonly={readonly}
                      />
                    </Box>
                  );
                } else {
                  console.warn('Unsupported property', property);
                  return <></>;
                }
              }
              default: {
                console.warn('Unsupported field type', property.type);
              }
            }
          }
        })}
    </Stack>
  );
}

export function JsonForm({
  schema,
  onSubmit,
  onChange,
  initial = {},
  hasName,
  error,
  readonly = false,
  button = 'Next',
  buttonColor = 'blue',
  inProgress = false,
  autocompleteData,
  autocompleteError,
}: {
  schema: JSONSchema7;
  onSubmit: (values: any) => Promise<void>;
  onChange?: (e: any) => void;
  initial?: any;
  hasName?: boolean;
  error: string | null;
  button?: string;
  readonly?: boolean;
  buttonColor?: string;
  inProgress?: boolean;
  autocompleteData?: {
    values: {
      [key: string]: string[] | undefined;
    };
  };
  autocompleteError?: string;
}) {
  let ajv = new Ajv();
  ajv.addKeyword('sensitive');
  ajv.addFormat('var-str', { validate: () => true });
  ajv.addFormat('autocomplete', { validate: () => true });
  const memoAjv = useMemo(() => addFormats(ajv), [schema]);

  const formik = useFormik({
    initialValues: initial,
    onSubmit,
    validate: values => {
      const errors: any = {};

      if (hasName && (!values.name || values.name.length == 0)) {
        errors.name = 'Name is required';
      }

      let validate = memoAjv.compile(schema);
      let valid = validate(values);

      if (!valid) {
        validate.errors?.forEach(error => {
          const path = error.instancePath.replace(/^\//, '').replace(/\//g, '.');
          errors[path] = error.message;
        });
      }

      return errors;
    },
  });

  const resetField = (field: string) => {
    formik.setFieldValue(field, undefined);
  };

  return (
    <form onSubmit={formik.handleSubmit}>
      {hasName && (
        <FormControl isRequired={true} pb={8}>
          <FormLabel>Name</FormLabel>
          <Input
            name="name"
            type="text"
            placeholder={`my-${schema.title?.toLowerCase()}`}
            value={formik.values.name || ''}
            onChange={e => {
              formik.handleChange(e);
              if (onChange) {
                onChange(formik.values);
              }
            }}
            readOnly={readonly}
          />
          <FormHelperText>Enter a name to identify this {schema.title || 'object'}</FormHelperText>
        </FormControl>
      )}

      <FormInner
        schema={schema}
        onChange={e => {
          formik.handleChange(e);
          if (onChange) {
            onChange(formik.values);
          }
        }}
        values={formik.values}
        errors={formik.errors}
        resetField={resetField}
        readonly={readonly}
        autocompleteData={autocompleteData}
        autocompleteError={autocompleteError}
      />

      {error && (
        <Alert mt={8} status="error">
          <AlertIcon />
          {error}
        </Alert>
      )}

      <Button
        colorScheme={buttonColor}
        mt={8}
        mb={4}
        type="submit"
        isLoading={formik.isSubmitting || inProgress}
      >
        {button}
      </Button>
    </form>
  );
}
