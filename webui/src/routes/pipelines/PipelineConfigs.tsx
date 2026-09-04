import {
  Alert,
  AlertIcon,
  Button,
  ButtonGroup,
  Flex,
  Heading,
  Spinner,
  Tab,
  TabList,
  TabPanel,
  TabPanels,
  Tabs,
} from '@chakra-ui/react';
import React, { ReactNode, useEffect, useMemo, useState } from 'react';

import { ConfigPath, ConfigViewer } from '../../components/ConfigViewer';
import { Job, useControllerConfig } from '../../lib/data_fetching';
import { formatError } from '../../lib/util';

type ConfigObject = Record<string, unknown>;

function isConfigObject(value: unknown): value is ConfigObject {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function cloneConfig(value: unknown): unknown {
  return value === undefined ? undefined : JSON.parse(JSON.stringify(value));
}

function asConfigObject(value: unknown): ConfigObject {
  return isConfigObject(value) ? (cloneConfig(value) as ConfigObject) : {};
}

function mergeConfig(base: unknown, overrides: unknown): unknown {
  if (!isConfigObject(base) || !isConfigObject(overrides)) {
    return cloneConfig(overrides);
  }

  const merged = asConfigObject(base);
  for (const [key, value] of Object.entries(overrides)) {
    merged[key] = key in base ? mergeConfig(base[key], value) : cloneConfig(value);
  }
  return merged;
}

function setConfigValue(config: ConfigObject, path: ConfigPath, value: unknown): ConfigObject {
  const next = asConfigObject(config);
  let current = next;

  path.slice(0, -1).forEach(part => {
    if (!isConfigObject(current[part])) {
      current[part] = {};
    }
    current = current[part] as ConfigObject;
  });

  current[path[path.length - 1]] = value;
  return next;
}

function removeConfigValue(config: ConfigObject, path: ConfigPath): ConfigObject {
  const next = asConfigObject(config);
  const parents: Array<{ parent: ConfigObject; key: string }> = [];
  let current = next;

  for (const part of path) {
    if (!Object.prototype.hasOwnProperty.call(current, part)) {
      return next;
    }
    parents.push({ parent: current, key: part });
    if (isConfigObject(current[part])) {
      current = current[part] as ConfigObject;
    }
  }

  const leaf = parents.pop();
  if (leaf) {
    delete leaf.parent[leaf.key];
  }

  parents.reverse().forEach(({ parent, key }) => {
    if (isConfigObject(parent[key]) && Object.keys(parent[key] as ConfigObject).length === 0) {
      delete parent[key];
    }
  });

  return next;
}

interface ConfigPanelProps {
  title: string;
  actions?: ReactNode;
  children: ReactNode;
}

function ConfigPanel({ title, actions, children }: ConfigPanelProps) {
  return (
    <TabPanel px={5} py={0}>
      <Flex align="start" justify="space-between" gap={4} mb={3}>
        <div>
          <Heading as="h3" fontSize="sm">
            {title}
          </Heading>
        </div>
        {actions}
      </Flex>
      {children}
    </TabPanel>
  );
}

export function PipelineConfigs({
  job,
  updatePipelineConfig,
}: {
  job: Job;
  updatePipelineConfig: (config: ConfigObject) => Promise<void>;
}) {
  const { controllerConfig, controllerConfigError, controllerConfigLoading } =
    useControllerConfig();
  const [isEditing, setIsEditing] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [saveError, setSaveError] = useState<string>();
  const overrideFingerprint = JSON.stringify(job.pipeline_config);
  const originalOverrides = useMemo(
    () => asConfigObject(job.pipeline_config),
    [overrideFingerprint]
  );
  const [draftOverrides, setDraftOverrides] = useState<ConfigObject>(() => originalOverrides);

  useEffect(() => {
    if (!isEditing) {
      setDraftOverrides(originalOverrides);
    }
  }, [isEditing, originalOverrides]);

  const controllerPipelineConfig = isConfigObject(controllerConfig)
    ? controllerConfig.pipeline
    : {};
  const effectivePipelineConfig = mergeConfig(controllerPipelineConfig, draftOverrides);
  const dirty = JSON.stringify(draftOverrides) !== JSON.stringify(originalOverrides);

  const startEditing = () => {
    setDraftOverrides(asConfigObject(originalOverrides));
    setSaveError(undefined);
    setIsEditing(true);
  };

  const cancelEditing = () => {
    setDraftOverrides(asConfigObject(originalOverrides));
    setSaveError(undefined);
    setIsEditing(false);
  };

  const save = async () => {
    setIsSaving(true);
    setSaveError(undefined);
    try {
      await updatePipelineConfig(draftOverrides);
      setIsEditing(false);
    } catch (error) {
      setSaveError(formatError(error));
    } finally {
      setIsSaving(false);
    }
  };

  const controllerContents = controllerConfigError ? (
    <Alert status="error" py={2} fontSize="xs">
      <AlertIcon boxSize={4} />
      {formatError(controllerConfigError)}
    </Alert>
  ) : controllerConfigLoading ? (
    <Spinner size="sm" />
  ) : (
    <ConfigViewer value={controllerConfig} />
  );

  return (
    <Tabs orientation="vertical" variant="unstyled" width="100%" height="100%" isLazy>
      <TabList
        width="190px"
        flexShrink={0}
        gap={1}
        pr={4}
        borderRightWidth="1px"
        borderColor="whiteAlpha.300"
      >
        {['Global', 'Pipeline', 'Scheduler', 'Environment Variables'].map(label => (
          <Tab
            key={label}
            justifyContent="flex-start"
            borderRadius="md"
            fontSize="sm"
            fontWeight="medium"
            px={3}
            py={2}
            textAlign="left"
            _hover={{ bg: 'whiteAlpha.100' }}
            _selected={{ bg: 'whiteAlpha.200', color: 'blue.200' }}
          >
            {label}
          </Tab>
        ))}
      </TabList>
      <TabPanels minWidth={0} overflow="auto">
        <ConfigPanel
          title="Global Config"
        >
          {controllerContents}
        </ConfigPanel>
        <ConfigPanel
          title="Pipeline Config"
          actions={
            isEditing ? (
              <ButtonGroup size="xs" flexShrink={0}>
                <Button variant="ghost" onClick={() => setDraftOverrides({})}>
                  Clear overrides
                </Button>
                <Button variant="ghost" onClick={cancelEditing}>
                  Cancel
                </Button>
                <Button colorScheme="blue" isDisabled={!dirty} isLoading={isSaving} onClick={save}>
                  Save
                </Button>
              </ButtonGroup>
            ) : (
              <Button
                size="xs"
                flexShrink={0}
                isDisabled={controllerConfigLoading || !!controllerConfigError}
                onClick={startEditing}
              >
                Edit
              </Button>
            )
          }
        >
          {saveError ? (
            <Alert status="error" py={2} mb={3} fontSize="xs">
              <AlertIcon boxSize={4} />
              {saveError}
            </Alert>
          ) : null}
          <ConfigViewer
            value={effectivePipelineConfig}
            overrides={draftOverrides}
            isEditing={isEditing}
            onChange={(path, value) =>
              setDraftOverrides(current => setConfigValue(current, path, value))
            }
            onReset={path => setDraftOverrides(current => removeConfigValue(current, path))}
          />
        </ConfigPanel>
        <ConfigPanel
          title="Scheduler Config Overrides"
        >
          <ConfigViewer
            value={job.scheduler_config}
            emptyMessage="This pipeline does not override the scheduler config."
          />
        </ConfigPanel>
        <ConfigPanel
          title="Environment Variable Overrides"
        >
          <ConfigViewer
            value={job.env_vars}
            emptyMessage="This pipeline does not define environment variable overrides."
          />
        </ConfigPanel>
      </TabPanels>
    </Tabs>
  );
}
