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
  IconButton,
  Input,
  Select,
  Stack,
  Textarea,
} from '@chakra-ui/react';
import { JSONSchema7 } from 'json-schema';
import { useFormik } from 'formik';

import Ajv from 'ajv';
import addFormats from 'ajv-formats';
import React, { useEffect, useMemo } from 'react';
import { AddIcon, DeleteIcon } from '@chakra-ui/icons';

function StringWidget({
  path,
  title,
  description,
  placeholder,
  required,
  maxLength,
  value,
  errors,
  onChange,
}: {
  path: string;
  title: string;
  description?: string;
  placeholder?: string;
  maxLength?: number;
  required?: boolean;
  value: string;
  errors: any;
  onChange: (e: React.ChangeEvent<any>) => void;
}) {
  return (
    <FormControl isRequired={required} isInvalid={errors[path]}>
      <FormLabel>{title}</FormLabel>
      {maxLength == null || maxLength < 100 ? (
        <Input
          name={path}
          type="text"
          placeholder={placeholder}
          value={value || ''}
          onChange={e => onChange(e)}
        />
      ) : (
        <Textarea
          name={path}
          placeholder={placeholder}
          value={value || ''}
          onChange={e => onChange(e)}
          resize={'vertical'}
          size={'md'}
        />
      )}
      {errors[path] ? (
        <FormErrorMessage>{errors[path]}</FormErrorMessage>
      ) : (
        description && <FormHelperText>{description}</FormHelperText>
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
      />
      {errors[path] ? (
        <FormErrorMessage>{errors[path]}</FormErrorMessage>
      ) : (
        description && <FormHelperText>{description}</FormHelperText>
      )}
    </FormControl>
  );
}

function SelectWidget({
  path,
  title,
  description,
  placeholder,
  required,
  options,
  value,
  onChange,
}: {
  path: string;
  title?: string;
  description?: string;
  placeholder?: string;
  required?: boolean;
  options: Array<{ value: string; label: string }>;
  value: string;
  onChange: (e: React.ChangeEvent<any>) => void;
}) {
  return (
    <FormControl isRequired={required}>
      {title && <FormLabel>{title}</FormLabel>}
      <Select
        placeholder={placeholder}
        name={path}
        isRequired={required}
        value={value}
        onChange={onChange}
        borderColor={'gray.600'}
      >
        {options.map(option => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </Select>
      {description && <FormHelperText>{description}</FormHelperText>}
    </FormControl>
  );
}

export function ArrayWidget({
  schema,
  onChange,
  path,
  values,
  errors,
}: {
  schema: JSONSchema7;
  onChange: (e: React.ChangeEvent<any>) => void;
  path: string;
  values: any;
  errors: any;
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
                <IconButton
                  width={8}
                  height={8}
                  minWidth={0}
                  aria-label="Delete item"
                  onClick={() => deleteItem(index)}
                  icon={<DeleteIcon width={3} />}
                />
              </Flex>
            ))}
            <IconButton mt={1} height={8} aria-label="Add item" onClick={add} icon={<AddIcon />} />
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
}: {
  schema: JSONSchema7;
  onChange: (e: React.ChangeEvent<any>) => void;
  path?: string;
  values: any;
  errors: any;
}) {
  useEffect(() => {
    if (!schema.properties || Object.keys(schema.properties).length == 0) {
      // @ts-ignore
      onChange({ target: { name: path, value: {} } });
    }
  }, [schema]);

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
          return !property.deprecated ?? true;
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
                      key={key}
                      title={property.title || key}
                      description={property.description}
                      placeholder="Select an option"
                      required={schema.required?.includes(key)}
                      options={property.enum.map(value => ({
                        value: value!.toString(),
                        label: value!.toString(),
                      }))}
                      value={traversePath(values, nextPath)}
                      onChange={onChange}
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
                      maxLength={property.maxLength}
                      // @ts-ignore
                      placeholder={property.examples ? (property.examples[0] as string) : undefined}
                      value={traversePath(values, nextPath)}
                      errors={errors}
                      onChange={onChange}
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
                          placeholder="Select an option"
                          description={property.description}
                          required={schema.required?.includes(key)}
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
                        />
                        {value != undefined && (
                          <Box p={4}>
                            <FormInner
                              path={nextPath}
                              key={key}
                              // @ts-ignore
                              schema={inSchema}
                              errors={errors}
                              onChange={onChange}
                              values={values}
                            />
                          </Box>
                        )}
                      </Stack>
                    </fieldset>
                  );
                } else if (property.properties != undefined) {
                  return (
                    <fieldset key={key} style={{ border: '1px solid #888', borderRadius: '8px' }}>
                      <legend
                        style={{ marginLeft: '8px', paddingLeft: '16px', paddingRight: '16px' }}
                      >
                        {property.title || key}
                      </legend>
                      <Box p={4}>
                        <FormInner
                          path={key}
                          // @ts-ignore
                          schema={property}
                          errors={errors}
                          onChange={onChange}
                          values={values}
                        />
                      </Box>
                    </fieldset>
                  );
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
  initial = {},
  hasName,
  error,
  button = 'Create',
}: {
  schema: JSONSchema7;
  onSubmit: (values: any) => Promise<void>;
  initial?: any;
  hasName?: boolean;
  error: string | null;
  button?: string;
}) {
  const ajv = useMemo(() => addFormats(new Ajv()), [schema]);

  const formik = useFormik({
    initialValues: initial,
    onSubmit,
    validate: values => {
      const errors: any = {};

      if (hasName && (!values.name || values.name.length == 0)) {
        errors.name = 'Name is required';
      }

      let validate = ajv.compile(schema);
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
            onChange={formik.handleChange}
          />
          <FormHelperText>Enter a name to identify this {schema.title || 'object'}</FormHelperText>
        </FormControl>
      )}

      <FormInner
        schema={schema}
        onChange={formik.handleChange}
        values={formik.values}
        errors={formik.errors}
      />

      {error && (
        <Alert mt={8} status="error">
          <AlertIcon />
          {error}
        </Alert>
      )}

      <Button mt={8} mb={4} type="submit" isLoading={formik.isSubmitting}>
        {button}
      </Button>
    </form>
  );
}
