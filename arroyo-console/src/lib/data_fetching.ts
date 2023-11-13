import useSWR, { mutate as globalMutate } from 'swr';
import useSWRInfinite from 'swr/infinite';

import { components, paths } from '../gen/api-types';
import createClient from 'openapi-fetch';

import { LocalUdf } from '../udf_state';
import { useRef } from 'react';

type schemas = components['schemas'];

export type Pipeline = schemas['Pipeline'];
export type Job = schemas['Job'];
export type StopType = schemas['StopType'];
export type PipelineGraph = schemas['PipelineGraph'];
export type JobLogMessage = schemas['JobLogMessage'];
export type PipelineNode = schemas['PipelineNode'];
export type OutputData = schemas['OutputData'];
export type MetricGroup = schemas['MetricGroup'];
export type Metric = schemas['Metric'];
export type OperatorMetricGroup = schemas['OperatorMetricGroup'];
export type Connector = schemas['Connector'];
export type Checkpoint = schemas['Checkpoint'];
export type ConnectionProfile = schemas['ConnectionProfile'];
export type ConnectionTable = schemas['ConnectionTable'];
export type Format = schemas['Format'];
export type TestSourceMessage = schemas['TestSourceMessage'];
export type ConnectionTablePost = schemas['ConnectionTablePost'];
export type ConnectionSchema = schemas['ConnectionSchema'];
export type SourceField = schemas['SourceField'];
export type OperatorCheckpointGroup = schemas['OperatorCheckpointGroup'];
export type SubtaskCheckpointGroup = schemas['SubtaskCheckpointGroup'];
export type CheckpointSpanType = schemas['CheckpointSpanType'];
export type GlobalUdf = schemas['GlobalUdf'];
export type PipelineLocalUdf = schemas['Udf'];
export type UdfValidationResult = schemas['UdfValidationResult'];

const BASE_URL = '/api';
export const { get, post, patch, del } = createClient<paths>({ baseUrl: BASE_URL });

const processResponse = (data: any | undefined, error: any | undefined) => {
  // SWR expects fetchers to throw errors, but openapi-fetch returns the error as a named field,
  // so this function throws the error if it exists
  if (error) {
    throw error;
  }
  return data;
};

// Keys

const connectorsKey = () => {
  return { key: 'Connectors' };
};

const connectionProfilesKey = () => {
  return { key: 'Connections' };
};

const connectionTablesKey = (limit: number) => {
  return (pageIndex: number, previousPageData: schemas['ConnectionTableCollection']) => {
    if (previousPageData && !previousPageData.hasMore) return null;

    if (pageIndex === 0) {
      return { key: 'ConnectionTables', startingAfter: undefined, limit };
    }

    return {
      key: 'ConnectionTables',
      startingAfter: previousPageData.data[previousPageData.data.length - 1].id,
      limit,
    };
  };
};

const pipelinesKey = (
  pageIndex: number,
  previousPageData: schemas['PipelineCollection'] | undefined
) => {
  if (previousPageData && !previousPageData.hasMore) return null;

  if (pageIndex === 0 || !previousPageData) {
    return { key: 'Piplines', startingAfter: undefined };
  }

  return {
    key: 'Piplines',
    startingAfter: previousPageData.data[previousPageData.data.length - 1].id,
  };
};

const jobMetricsKey = (pipelineId?: string, jobId?: string) => {
  return pipelineId && jobId ? { key: 'JobMetrics', pipelineId, jobId } : null;
};

const jobCheckpointsKey = (pipelineId?: string, jobId?: string) => {
  return pipelineId && jobId ? { key: 'JobCheckpoints', pipelineId, jobId } : null;
};

const checkpointDetailsKey = (pipelineId?: string, jobId?: string, epoch?: number) => {
  return pipelineId && jobId && epoch
    ? { key: 'CheckpointDetails', pipelineId, jobId, epoch }
    : null;
};

const operatorErrorsKey = (pipelineId?: string, jobId?: string) => {
  return (pageIndex: number, previousPageData: schemas['JobLogMessageCollection']) => {
    if (!pipelineId || !jobId) return null;
    if (previousPageData && !previousPageData.hasMore) return null;

    if (pageIndex === 0) {
      return { key: 'OperatorErrors', pipelineId, jobId, startingAfter: undefined };
    }

    return {
      key: 'OperatorErrors',
      pipelineId,
      jobId,
      startingAfter: previousPageData.data[previousPageData.data.length - 1].id,
    };
  };
};

const queryValidationKey = (query?: string, localUdfs?: LocalUdf[]) => {
  return query != undefined ? { key: 'PipelineGraph', query, localUdfs } : null;
};

const udfValidationKey = (definition: string) => {
  return { key: 'UdfValidation', definition };
};

const pipelineKey = (pipelineId?: string) => {
  return pipelineId ? { key: 'Pipeline', pipelineId } : null;
};

const pipelineJobsKey = (pipelineId?: string) => {
  return pipelineId ? { key: 'PipelineJobs', pipelineId } : null;
};

// Ping

const pingFetcher = async () => {
  const { data, error } = await get('/v1/ping', {});
  return processResponse(data, error);
};

export const usePing = () => {
  const { data, error, isLoading } = useSWR('ping', pingFetcher, {
    refreshInterval: 1000,
    onErrorRetry: (error, key, config, revalidate, {}) => {
      // explicitly define this function to override the exponential backoff
      setTimeout(() => revalidate(), 1000);
    },
  });

  return {
    ping: data,
    pingLoading: isLoading,
    pingError: error,
  };
};

// Connectors
const connectorsFetcher = async () => {
  const { data, error } = await get('/v1/connectors', {});
  return processResponse(data, error);
};

export const useConnectors = () => {
  const { data, isLoading } = useSWR<schemas['ConnectorCollection']>(
    connectorsKey(),
    connectorsFetcher
  );

  return {
    connectors: data?.data,
    connectorsLoading: isLoading,
  };
};

// Connections
const connectionProfilesFetcher = async () => {
  const { data, error } = await get('/v1/connection_profiles', {});
  return processResponse(data, error);
};

export const useConnectionProfiles = () => {
  const { data, isLoading, mutate } = useSWR<schemas['ConnectionProfileCollection']>(
    connectionProfilesKey(),
    connectionProfilesFetcher
  );

  return {
    connectionProfiles: data?.data,
    connectionProfilesLoading: isLoading,
    mutateConnectionProfiles: mutate,
  };
};

// ConnectionTables
const connectionTablesFetcher = () => {
  return async (params: { key: string; startingAfter?: string; limit: number }) => {
    const { data, error } = await get('/v1/connection_tables', {
      params: {
        query: {
          starting_after: params.startingAfter,
          limit: params.limit,
        },
      },
    });

    return processResponse(data, error);
  };
};

export const useConnectionTables = (limit: number) => {
  const { data, isLoading, mutate, size, setSize } = useSWRInfinite<
    schemas['ConnectionTableCollection']
  >(connectionTablesKey(limit), connectionTablesFetcher(), {
    refreshInterval: 5000,
  });

  return {
    connectionTablePages: data,
    connectionTablesLoading: isLoading,
    mutateConnectionTables: mutate,
    connectionTablesTotalPages: size,
    setConnectionTablesMaxPages: setSize,
  };
};

// Jobs

const jobsFetcher = async () => {
  const { data, error } = await get('/v1/jobs', {});
  return processResponse(data, error);
};

export const useJobs = () => {
  const { data, isLoading } = useSWR<schemas['JobCollection']>('jobs', jobsFetcher);
  return {
    jobs: data?.data,
    jobsLoading: isLoading,
  };
};

// Job Metrics

const jobMetricsFetcher = () => {
  return async (params: { key: string; pipelineId: string; jobId: string }) => {
    const { data, error } = await get(
      '/v1/pipelines/{pipeline_id}/jobs/{job_id}/operator_metric_groups',
      {
        params: {
          path: {
            pipeline_id: params.pipelineId,
            job_id: params.jobId,
          },
        },
      }
    );

    return processResponse(data, error);
  };
};

export const useJobMetrics = (pipelineId?: string, jobId?: string) => {
  const { data, isLoading, error } = useSWR<schemas['OperatorMetricGroupCollection']>(
    jobMetricsKey(pipelineId, jobId),
    jobMetricsFetcher(),
    {
      refreshInterval: 1000,
    }
  );

  return {
    operatorMetricGroups: data?.data,
    operatorMetricGroupsLoading: isLoading,
    operatorMetricGroupsError: error,
  };
};

// JobCheckpointsReq

const jobCheckpointsFetcher = () => {
  return async (params: { key: string; pipelineId: string; jobId: string }) => {
    const { data, error } = await get('/v1/pipelines/{pipeline_id}/jobs/{job_id}/checkpoints', {
      params: {
        path: {
          pipeline_id: params.pipelineId,
          job_id: params.jobId,
        },
      },
    });

    return processResponse(data, error);
  };
};

export const useJobCheckpoints = (pipelineId?: string, jobId?: string) => {
  const { data } = useSWR<schemas['CheckpointCollection']>(
    jobCheckpointsKey(pipelineId, jobId),
    jobCheckpointsFetcher(),
    {
      refreshInterval: 5000,
    }
  );

  return {
    checkpoints: data?.data,
  };
};

// CheckpointDetailsReq

const checkpointDetailsFetcher = () => {
  return async (params: { key: string; pipelineId: string; jobId: string; epoch: number }) => {
    const { data, error } = await get(
      '/v1/pipelines/{pipeline_id}/jobs/{job_id}/checkpoints/{epoch}/operator_checkpoint_groups',
      {
        params: {
          path: { pipeline_id: params.pipelineId, job_id: params.jobId, epoch: params.epoch },
        },
      }
    );
    return processResponse(data, error);
  };
};

export const useCheckpointDetails = (pipelineId?: string, jobId?: string, epoch?: number) => {
  const { data, isLoading } = useSWR<schemas['OperatorCheckpointGroupCollection']>(
    checkpointDetailsKey(pipelineId, jobId, epoch),
    checkpointDetailsFetcher()
  );

  return {
    checkpointDetails: data?.data,
    checkpointLoading: isLoading,
  };
};

const queryValidationFetcher = () => {
  return async (params: { key: string; query?: string; localUdfs?: LocalUdf[] }) => {
    let udfs: PipelineLocalUdf[] = [];
    if (params.localUdfs) {
      udfs = params.localUdfs.map(udf => {
        return { definition: udf.definition };
      });
    }

    const { data, error } = await post('/v1/pipelines/validate_query', {
      body: {
        query: params.query ?? '',
        udfs: udfs,
      },
    });
    return processResponse(data, error);
  };
};

export const useQueryValidation = (query?: string, localUdfs?: LocalUdf[]) => {
  const { data, error, isLoading } = useSWR<schemas['QueryValidationResult']>(
    queryValidationKey(query, localUdfs),
    queryValidationFetcher(),
    { revalidateOnFocus: false, revalidateIfStale: false, shouldRetryOnError: false }
  );

  return {
    queryValidation: data,
    queryValidationError: error,
    queryValidationLoading: isLoading,
  };
};

const udfValidationFetcher = () => {
  const controller = useRef<AbortController>();
  return async (params: { key: string; definition: string }) => {
    controller.current?.abort();
    controller.current = new AbortController();
    const { data, error } = await post('/v1/udfs/validate', {
      body: {
        definition: params.definition,
      },
      signal: controller.current?.signal,
    });
    return processResponse(data, error);
  };
};

export const useUdfValidation = (
  onSuccess: (data: UdfValidationResult, key: any, config: any) => void,
  definition: string
) => {
  const { data, error, isLoading } = useSWR<schemas['UdfValidationResult']>(
    udfValidationKey(definition),
    udfValidationFetcher(),
    { revalidateOnFocus: false, revalidateIfStale: false, shouldRetryOnError: false, onSuccess }
  );

  return {
    udfValidation: data,
    udfValidationError: error,
    udfValidationLoading: isLoading,
  };
};

const pipelinesFetcher = () => {
  return async (params: { key: string; startingAfter?: string }) => {
    const { data, error } = await get('/v1/pipelines', {
      params: {
        query: {
          starting_after: params.startingAfter,
        },
      },
    });

    return processResponse(data, error);
  };
};

export const usePipelines = () => {
  const { data, isLoading, error, size, setSize } = useSWRInfinite<schemas['PipelineCollection']>(
    pipelinesKey,
    pipelinesFetcher(),
    {
      refreshInterval: 2000,
    }
  );

  return {
    pipelinePages: data,
    pipelinesLoading: isLoading,
    piplinesError: error,
    pipelineTotalPages: size,
    setPipelinesMaxPages: setSize,
  };
};

const pipelineFetcher = () => {
  return async (params: { key: string; pipelineId?: string }) => {
    if (!params.pipelineId) {
      return;
    }

    const { data, error } = await get(`/v1/pipelines/{id}`, {
      params: { path: { id: params.pipelineId } },
    });
    return processResponse(data, error);
  };
};

export const usePipeline = (pipelineId?: string, refresh: boolean = false) => {
  const options = refresh ? { refreshInterval: 2000 } : {};
  const { data, error, isLoading, mutate } = useSWR<schemas['Pipeline']>(
    pipelineKey(pipelineId),
    pipelineFetcher(),
    options
  );

  const updatePipeline = async (params: { stop?: StopType; parallelism?: number }) => {
    if (!pipelineId) {
      return;
    }

    await patch('/v1/pipelines/{id}', {
      params: { path: { id: pipelineId } },
      body: { stop: params.stop, parallelism: params.parallelism },
    });
    await mutate();
  };

  const restartPipeline = async () => {
    if (!pipelineId) {
      return;
    }

    await post('/v1/pipelines/{id}/restart', {
      params: { path: { id: pipelineId } },
      body: {},
    });
    await mutate();
  };

  const deletePipeline = async () => {
    if (!pipelineId) {
      return { error: undefined };
    }

    const { error } = await del('/v1/pipelines/{id}', {
      params: { path: { id: pipelineId } },
    });
    await globalMutate(pipelinesKey(1, undefined));
    return { error };
  };

  return {
    pipeline: data,
    pipelineError: error,
    pipelineLoading: isLoading,
    updatePipeline,
    deletePipeline,
    restartPipeline,
  };
};

const pipelineJobsFetcher = () => {
  return async (params: { key: string; pipelineId?: string }) => {
    if (!params.pipelineId) {
      return;
    }

    const { data, error } = await get(`/v1/pipelines/{id}/jobs`, {
      params: { path: { id: params.pipelineId } },
    });
    return processResponse(data, error);
  };
};

export const usePipelineJobs = (pipelineId?: string, refresh: boolean = false) => {
  const options = refresh ? { refreshInterval: 2000 } : {};
  const { data, error } = useSWR<schemas['JobCollection']>(
    pipelineJobsKey(pipelineId),
    pipelineJobsFetcher(),
    options
  );

  return { jobs: data?.data, jobsError: error };
};

const operatorErrorsFetcher = () => {
  return async (params: {
    key: string;
    pipelineId: string;
    jobId: string;
    startingAfter?: string;
  }) => {
    const { data, error } = await get('/v1/pipelines/{pipeline_id}/jobs/{job_id}/errors', {
      params: {
        path: {
          pipeline_id: params.pipelineId,
          job_id: params.jobId,
        },
        query: {
          starting_after: params.startingAfter,
        },
      },
    });

    return processResponse(data, error);
  };
};

export const useOperatorErrors = (pipelineId?: string, jobId?: string) => {
  const { data, isLoading, size, setSize } = useSWRInfinite<schemas['JobLogMessageCollection']>(
    operatorErrorsKey(pipelineId, jobId),
    operatorErrorsFetcher(),
    {
      refreshInterval: 5000,
    }
  );

  return {
    operatorErrorsPages: data,
    operatorErrorsLoading: isLoading,
    operatorErrorsTotalPages: size,
    setOperatorErrorsMaxPages: setSize,
  };
};

export const useJobOutput = (
  handler: (event: MessageEvent) => void,
  pipelineId?: string,
  jobId?: string
) => {
  if (!pipelineId || !jobId) {
    return;
  }
  const url = `${BASE_URL}/v1/pipelines/${pipelineId}/jobs/${jobId}/output`;
  const eventSource = new EventSource(url);
  eventSource.onerror = () => {
    eventSource.close();
  };
  eventSource.addEventListener('message', handler);
  return eventSource;
};

export const useConnectionTableTest = async (
  handler: (event: TestSourceMessage) => void,
  req: ConnectionTablePost
) => {
  const url = `${BASE_URL}/v1/connection_tables/test`;
  const response = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Accept: 'text/event-stream',
    },
    body: JSON.stringify(req),
  });

  if (!response.ok || !response.body) {
    const text = await response.text();
    let message;
    try {
      message = (JSON.parse(text) as { error: string }).error;
    } catch {
      message = text;
    }
    handler({
      done: true,
      error: true,
      message,
    });
    return;
  }

  const reader = response.body.getReader();
  const textDecoder = new TextDecoder();

  let buffer = '';
  while (true) {
    const { value, done } = await reader.read();

    if (done) {
      break;
    }

    buffer += textDecoder.decode(value, { stream: true });
    const lines = buffer.split('\n');
    buffer = lines.pop() || '';

    for (const line of lines) {
      const value = line.substring('data:'.length);
      if (value) {
        try {
          const parsed = JSON.parse(value) as TestSourceMessage;
          handler(parsed);
        } catch {}
      }
    }
  }
};

const udfsFetcher = async () => {
  const { data, error } = await get('/v1/udfs', {});
  return processResponse(data, error);
};

export const useGlobalUdfs = () => {
  const { data, isLoading, error, mutate } = useSWR<schemas['GlobalUdfCollection']>(
    'udfs',
    udfsFetcher
  );

  const createGlobalUdf = async (prefix: string, definition: string, description: string) => {
    const { data, error } = await post('/v1/udfs', {
      body: {
        prefix,
        definition,
        description,
      },
    });
    await mutate();
    return { data, error };
  };

  const deleteGlobalUdf = async (udf: GlobalUdf) => {
    const { error } = await del('/v1/udfs/{id}', {
      params: { path: { id: udf.id } },
    });
    await mutate();
    return { error };
  };

  return {
    globalUdfs: data?.data,
    udfsLoading: isLoading,
    udfError: error,
    createGlobalUdf,
    deleteGlobalUdf,
  };
};
