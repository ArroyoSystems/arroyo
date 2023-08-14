import { CheckpointDetailsReq, GetConnectionsReq, GetConnectionTablesReq } from '../gen/api_pb';
import { ApiClient } from '../main';
import useSWR, { mutate as globalMutate } from 'swr';
import { components, paths } from '../gen/api-types';
import createClient from 'openapi-fetch';

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

const connectionsKey = () => {
  return { key: 'Connections' };
};

const connectionTablesKey = () => {
  return { key: 'ConnectionTables' };
};

const jobMetricsKey = (pipelineId?: string, jobId?: string) => {
  return pipelineId && jobId ? { key: 'JobMetrics', pipelineId, jobId } : null;
};

const jobCheckpointsKey = (pipelineId?: string, jobId?: string) => {
  return pipelineId && jobId ? { key: 'JobCheckpoints', pipelineId, jobId } : null;
};

const checkpointDetailsKey = (jobId?: string, epoch?: number) => {
  return jobId && epoch ? { key: 'CheckpointDetails', jobId, epoch } : null;
};

const operatorErrorsKey = (pipelineId?: string, jobId?: string) => {
  return pipelineId && jobId ? { key: 'JobEvents', pipelineId, jobId } : null;
};

const pipelineGraphKey = (query?: string, udfsInput?: string) => {
  return query ? { key: 'PipelineGraph', query, udfsInput } : null;
};

const pipelinesKey = () => {
  return { key: 'Pipelines' };
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
  const { data, error } = useSWR('ping', pingFetcher, {
    refreshInterval: 1000,
    onErrorRetry: (error, key, config, revalidate, {}) => {
      // explicitly define this function to override the exponential backoff
      setTimeout(() => revalidate(), 1000);
    },
  });

  return {
    ping: data,
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
const connectionsFetcher = (client: ApiClient) => {
  return async () => {
    return (await (await client()).getConnections(new GetConnectionsReq({}))).connections;
  };
};

export const useConnections = (client: ApiClient) => {
  const { data, isLoading, mutate } = useSWR(connectionsKey(), connectionsFetcher(client));

  return {
    connections: data,
    connectionsLoading: isLoading,
    mutateConnections: mutate,
  };
};

// ConnectionTables
const connectionTablesFetcher = (client: ApiClient) => {
  return async () => {
    return (await (await client()).getConnectionTables(new GetConnectionTablesReq({}))).tables;
  };
};

export const useConnectionTables = (client: ApiClient) => {
  const { data, isLoading, mutate } = useSWR(
    connectionTablesKey(),
    connectionTablesFetcher(client),
    {
      refreshInterval: 5000,
    }
  );

  return {
    connectionTables: data,
    connectionTablesLoading: isLoading,
    mutateConnectionTables: mutate,
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
  const { data, error } = useSWR<schemas['OperatorMetricGroupCollection']>(
    jobMetricsKey(pipelineId, jobId),
    jobMetricsFetcher(),
    {
      refreshInterval: 1000,
    }
  );

  return {
    operatorMetricGroups: data?.data,
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

const checkpointDetailsFetcher = (client: ApiClient) => {
  return async (params: { key: string; jobId: string; epoch: number }) => {
    return await (
      await (
        await client
      )()
    ).getCheckpointDetail(
      new CheckpointDetailsReq({
        jobId: params.jobId,
        epoch: params.epoch,
      })
    );
  };
};

export const useCheckpointDetails = (client: ApiClient, jobId?: string, epoch?: number) => {
  const { data, isLoading } = useSWR(
    checkpointDetailsKey(jobId, epoch),
    checkpointDetailsFetcher(client)
  );

  return {
    checkpoint: data,
    checkpointLoading: isLoading,
  };
};

const pipelineGraphFetcher = () => {
  return async (params: { key: string; query?: string; udfsInput: string }) => {
    const { data, error } = await post('/v1/pipelines/validate', {
      body: {
        query: params.query ?? '',
        udfs: [{ language: 'rust', definition: params.udfsInput }],
      },
    });
    return processResponse(data, error);
  };
};

export const usePipelineGraph = (query?: string, udfsInput?: string) => {
  const { data, error } = useSWR<schemas['PipelineGraph']>(
    pipelineGraphKey(query, udfsInput),
    pipelineGraphFetcher()
  );

  return {
    pipelineGraph: data,
    pipelineGraphError: error,
  };
};

const pipelinesFetcher = async () => {
  const { data, error } = await get('/v1/pipelines', {});
  return processResponse(data, error);
};

export const usePipelines = () => {
  const { data, error } = useSWR<schemas['PipelineCollection']>(pipelinesKey(), pipelinesFetcher, {
    refreshInterval: 2000,
  });
  return { pipelines: data?.data, piplinesError: error };
};

const pipelineFetcher = () => {
  return async (params: { key: string; pipelineId?: string }) => {
    const { data, error } = await get(`/v1/pipelines/{id}`, {
      params: { path: { id: params.pipelineId! } },
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
    await patch('/v1/pipelines/{id}', {
      params: { path: { id: pipelineId! } },
      body: { stop: params.stop, parallelism: params.parallelism },
    });
    await mutate();
  };

  const deletePipeline = async () => {
    const { error } = await del('/v1/pipelines/{id}', {
      params: { path: { id: pipelineId! } },
    });
    await globalMutate(pipelinesKey());
    return { error };
  };

  return {
    pipeline: data,
    pipelineError: error,
    pipelineLoading: isLoading,
    updatePipeline,
    deletePipeline,
  };
};

const pipelineJobsFetcher = () => {
  return async (params: { key: string; pipelineId?: string }) => {
    const { data, error } = await get(`/v1/pipelines/{id}/jobs`, {
      params: { path: { id: params.pipelineId! } },
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
  return async (params: { key: string; pipelineId: string; jobId: string }) => {
    const { data, error } = await get('/v1/pipelines/{pipeline_id}/jobs/{job_id}/errors', {
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

export const useOperatorErrors = (pipelineId?: string, jobId?: string) => {
  const { data } = useSWR<schemas['JobLogMessageCollection']>(
    operatorErrorsKey(pipelineId, jobId),
    operatorErrorsFetcher(),
    {
      refreshInterval: 5000,
    }
  );

  return {
    operatorErrors: data?.data,
  };
};

export const useJobOutput = (
  handler: (event: MessageEvent) => void,
  pipelineId?: string,
  jobId?: string
) => {
  const url = `${BASE_URL}/v1/pipelines/${pipelineId}/jobs/${jobId}/output`;
  const eventSource = new EventSource(url);
  eventSource.addEventListener('message', handler);
};
