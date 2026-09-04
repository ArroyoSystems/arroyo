import { Badge, Box, Button, Flex, Grid, Input, Select, Stack, Text } from '@chakra-ui/react';
import React, { useRef } from 'react';

type ConfigObject = Record<string, unknown>;
export type ConfigPath = string[];

function isConfigObject(value: unknown): value is ConfigObject {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function isEmpty(value: unknown): boolean {
  return (
    value == null ||
    (Array.isArray(value) && value.length === 0) ||
    (isConfigObject(value) && Object.keys(value).length === 0)
  );
}

function hasConfigPath(value: unknown, path: ConfigPath): boolean {
  let current = value;

  for (const part of path) {
    if (!isConfigObject(current) || !Object.prototype.hasOwnProperty.call(current, part)) {
      return false;
    }
    current = current[part];
  }

  return true;
}

function ScalarValue({ value }: { value: unknown }) {
  if (value === null) {
    return (
      <Text as="span" fontSize="xs" fontStyle="italic">
        null
      </Text>
    );
  }

  return (
    <Text as="span" fontFamily="mono" fontSize="xs" overflowWrap="anywhere">
      {String(value)}
    </Text>
  );
}

function EditableScalar({
  value,
  onChange,
}: {
  value: unknown;
  onChange: (value: unknown) => void;
}) {
  const valueType = useRef(value === null ? 'string' : typeof value);

  if (valueType.current === 'boolean') {
    return (
      <Select
        size="xs"
        height={6}
        maxWidth="120px"
        fontFamily="mono"
        fontSize="xs"
        value={String(value)}
        onChange={event => onChange(event.target.value === 'true')}
      >
        <option value="true">true</option>
        <option value="false">false</option>
      </Select>
    );
  }

  return (
    <Input
      size="xs"
      height={6}
      maxWidth="360px"
      fontFamily="mono"
      fontSize="xs"
      type={valueType.current === 'number' ? 'number' : 'text'}
      value={value === null ? '' : String(value)}
      onChange={event => {
        const input = event.target.value;
        if (valueType.current !== 'number' || input === '' || Number.isNaN(Number(input))) {
          onChange(input);
        } else {
          onChange(Number(input));
        }
      }}
    />
  );
}

interface ConfigEntryProps {
  name: string;
  value: unknown;
  path: ConfigPath;
  overrides?: unknown;
  isEditing?: boolean;
  onChange?: (path: ConfigPath, value: unknown) => void;
  onReset?: (path: ConfigPath) => void;
}

function ConfigEntry({
  name,
  value,
  path,
  overrides,
  isEditing,
  onChange,
  onReset,
}: ConfigEntryProps) {
  const nested = Array.isArray(value) || isConfigObject(value);
  const overridden = hasConfigPath(overrides, path);

  if (!nested || value === null) {
    return (
      <Grid
        templateColumns={{ base: 'minmax(120px, 35%) 1fr', xl: '280px 1fr' }}
        gap={4}
        px={3}
        py={2}
        borderBottomWidth="1px"
        borderColor="whiteAlpha.200"
        _last={{ borderBottomWidth: 0 }}
      >
        <Flex align="center" gap={2} minWidth={0}>
          <Text fontFamily="mono" fontSize="xs" fontWeight="semibold" overflowWrap="anywhere">
            {name}
          </Text>
          {overridden ? (
            <Badge variant="outline" flexShrink={0} px={1} fontSize="8px" lineHeight={4}>
              override
            </Badge>
          ) : null}
        </Flex>
        <Flex align="center" gap={2} minWidth={0}>
          {isEditing && onChange ? (
            <EditableScalar value={value} onChange={next => onChange(path, next)} />
          ) : (
            <ScalarValue value={value} />
          )}
          {isEditing && overridden && onReset ? (
            <Button
              flexShrink={0}
              height={6}
              minWidth="auto"
              px={2}
              size="xs"
              variant="ghost"
              onClick={() => onReset(path)}
            >
              Reset
            </Button>
          ) : null}
        </Flex>
      </Grid>
    );
  }

  const entries: [string, unknown][] = Array.isArray(value)
    ? value.map((item, index) => [`[${index}]`, item])
    : Object.entries(value);

  return (
    <Box px={3} py={2} borderBottomWidth="1px" borderColor="whiteAlpha.200" _last={{ border: 0 }}>
      <Text fontFamily="mono" fontSize="xs" fontWeight="semibold" overflowWrap="anywhere">
        {name}
      </Text>
      {entries.length === 0 ? (
        <Text mt={1} ml={3} color="gray.500" fontFamily="mono" fontSize="xs">
          {Array.isArray(value) ? '[]' : '{}'}
        </Text>
      ) : (
        <Box mt={1} ml={2} borderLeftWidth="1px" borderColor="whiteAlpha.300">
          {entries.map(([key, child]) => (
            <ConfigEntry
              key={[...path, key].join('.')}
              name={key}
              value={child}
              path={[...path, key]}
              overrides={overrides}
              isEditing={isEditing}
              onChange={onChange}
              onReset={onReset}
            />
          ))}
        </Box>
      )}
    </Box>
  );
}

export function ConfigViewer({
  value,
  emptyMessage = 'No configuration values.',
  overrides,
  isEditing = false,
  onChange,
  onReset,
}: {
  value: unknown;
  emptyMessage?: string;
  overrides?: unknown;
  isEditing?: boolean;
  onChange?: (path: ConfigPath, value: unknown) => void;
  onReset?: (path: ConfigPath) => void;
}) {
  if (isEmpty(value)) {
    return (
      <Text color="gray.500" fontSize="sm" fontStyle="italic">
        {emptyMessage}
      </Text>
    );
  }

  const entries: [string, unknown][] = Array.isArray(value)
    ? value.map((item, index) => [`[${index}]`, item])
    : isConfigObject(value)
    ? Object.entries(value)
    : [['value', value]];

  return (
    <Stack spacing={0} borderWidth="1px" borderColor="whiteAlpha.300" borderRadius="md">
      {entries.map(([key, child]) => (
        <ConfigEntry
          key={key}
          name={key}
          value={child}
          path={[key]}
          overrides={overrides}
          isEditing={isEditing}
          onChange={onChange}
          onReset={onReset}
        />
      ))}
    </Stack>
  );
}
