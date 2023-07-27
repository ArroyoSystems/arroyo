import {
  CheckpointDetailsReq,
  GetConnectionsReq,
  GetConnectionTablesReq,
  GetConnectorsReq,
  JobMetricsReq,
} from '../gen/api_pb';
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

const BASE_URL = 'http://localhost:8000/api';
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

const jobMetricsKey = (jobId?: string) => {
  return jobId ? { key: 'JobMetrics', jobId } : null;
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
const connectorsFetcher = (client: ApiClient) => {
  return async () => {
    return await (await client()).getConnectors(new GetConnectorsReq({}));
  };
};

export const useConnectors = (client: ApiClient) => {
  const { data, isLoading } = useSWR(connectorsKey(), connectorsFetcher(client));

  return {
    connectors: data,
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
  const { data, isLoading } = useSWR('jobs', jobsFetcher);
  return {
    jobs: data?.data,
    jobsLoading: isLoading,
  };
};

// JobMetricsReq

const jobMetricsFetcher = (client: ApiClient) => {
  return async (params: { jobId: string }) => {
    return await (
      await (
        await client
      )()
    ).getJobMetrics(
      new JobMetricsReq({
        jobId: params.jobId,
      })
    );
  };
};

export const useJobMetrics = (client: ApiClient, jobId?: string) => {
  const { data } = useSWR(jobMetricsKey(jobId), jobMetricsFetcher(client), {
    refreshInterval: 5000,
  });

  return {
    metrics: data,
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
  const { data } = useSWR(jobCheckpointsKey(pipelineId, jobId), jobCheckpointsFetcher(), {
    refreshInterval: 5000,
  });

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
    const { data, error } = await post('/v1/validate', {
      body: {
        query: params.query ?? '',
        udfs: [{ language: 'rust', definition: params.udfsInput }],
      },
    });
    return processResponse(data, error);
  };
};

export const usePipelineGraph = (query?: string, udfsInput?: string) => {
  const { data, error } = useSWR(pipelineGraphKey(query, udfsInput), pipelineGraphFetcher());

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
  const { data, error } = useSWR(pipelinesKey(), pipelinesFetcher, { refreshInterval: 2000 });
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
  const { data, error, isLoading, mutate } = useSWR(
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
  const { data, error } = useSWR(pipelineJobsKey(pipelineId), pipelineJobsFetcher(), options);

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
  const { data } = useSWR(operatorErrorsKey(pipelineId, jobId), operatorErrorsFetcher(), {
    refreshInterval: 5000,
  });

  return {
    operatorErrors: data?.data,
  };
};
