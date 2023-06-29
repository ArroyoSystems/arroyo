import createClient from 'openapi-fetch';
import {
  BuiltinSink,
  CheckpointDetailsReq,
  CreateUdf,
  GetSinksReq,
  GetSourcesReq,
  JobCheckpointsReq,
  JobDetailsReq,
  JobMetricsReq,
  OperatorErrorsReq,
  PipelineGraphReq,
  StopType,
  UdfLanguage,
} from '../gen/api_pb';
import { ApiClient } from '../main';
import useSWR from 'swr';
import { mutate as globalMutate } from 'swr';
import { SinkOpt } from './types';
import { components, paths } from '../gen/api-types';

export type PostConnections = components['schemas']['PostConnections'];

const BASE_URL = 'http://localhost:8003';
const { get, post } = createClient<paths>({ baseUrl: BASE_URL });

// Keys

const jobDetailsKey = (jobId?: string) => {
  return jobId ? { key: 'Job', jobId } : null;
};

const jobMetricsKey = (jobId?: string) => {
  return jobId ? { key: 'JobMetrics', jobId } : null;
};

const jobCheckpointsKey = (jobId?: string) => {
  return jobId ? { key: 'JobCheckpoints', jobId } : null;
};

const checkpointDetailsKey = (jobId?: string, epoch?: number) => {
  return jobId ? { key: 'CheckpointDetails', jobId, epoch } : null;
};

const operatorErrorsKey = (jobId?: string) => {
  return jobId ? { key: 'JobEvents', jobId } : null;
};

const pipelineGraphKey = (query?: string, udfInput?: string) => {
  return query ? { key: 'PipelineGraph', query, udfInput } : null;
};

const sourcesKey = () => {
  return { key: 'Sources' };
};

const sinksKey = () => {
  return { key: 'Sinks' };
};

const connectionsKey = () => {
  return { key: 'Connections' };
};

const connectionKey = (connectionId?: number) => {
  return connectionId ? { key: 'Connection', connectionId } : null;
};

// JobDetailsReq

const jobFetcher = (client: ApiClient) => {
  return async (params: { jobId: string }) => {
    return await (
      await (
        await client
      )()
    ).getJobDetails(
      new JobDetailsReq({
        jobId: params.jobId,
      })
    );
  };
};

export const useJob = (client: ApiClient, jobId?: string) => {
  const { data, mutate, error } = useSWR(jobDetailsKey(jobId), jobFetcher(client), {
    refreshInterval: 2000,
  });

  const updateJob = async (params: { parallelism?: number; stop?: StopType }) => {
    await (
      await (
        await client
      )()
    ).updateJob({
      jobId,
      parallelism: params.parallelism,
      stop: params.stop,
    });
    mutate();
    globalMutate(jobMetricsKey(jobId));
  };

  return {
    job: data,
    jobError: error,
    updateJob,
    refreshJob: mutate,
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
  const { data, isLoading, mutate, isValidating, error } = useSWR(
    jobMetricsKey(jobId),
    jobMetricsFetcher(client),
    {
      refreshInterval: 5000,
    }
  );

  return {
    metrics: data,
  };
};

// JobCheckpointsReq

const jobCheckpointsFetcher = (client: ApiClient) => {
  return async (params: { key: string; jobId: string }) => {
    return await (
      await (
        await client
      )()
    ).getCheckpoints(
      new JobCheckpointsReq({
        jobId: params.jobId,
      })
    );
  };
};

export const useJobCheckpoints = (client: ApiClient, jobId?: string) => {
  const { data } = useSWR(jobCheckpointsKey(jobId), jobCheckpointsFetcher(client), {
    refreshInterval: 5000,
  });

  return {
    checkpoints: data,
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

// OperatorErrorsReq

const OperatorErrorsFetcher = (client: ApiClient) => {
  return async (params: { key: string; jobId: string }) => {
    return await (
      await (
        await client
      )()
    ).getOperatorErrors(
      new OperatorErrorsReq({
        jobId: params.jobId,
      })
    );
  };
};

export const useOperatorErrors = (client: ApiClient, jobId?: string) => {
  const { data } = useSWR(operatorErrorsKey(jobId), OperatorErrorsFetcher(client), {
    refreshInterval: 5000,
  });

  return {
    operatorErrors: data,
  };
};

// PipelineGraphReq

const PipelineGraphFetcher = (client: ApiClient) => {
  return async (params: { key: string; query?: string; udfInput: string }) => {
    return await (
      await (
        await client
      )()
    ).graphForPipeline(
      new PipelineGraphReq({
        query: params.query,
        udfs: [new CreateUdf({ language: UdfLanguage.Rust, definition: params.udfInput })],
      })
    );
  };
};

export const usePipelineGraph = (client: ApiClient, query?: string, udfInput?: string) => {
  const { data } = useSWR(pipelineGraphKey(query, udfInput), PipelineGraphFetcher(client));

  return {
    pipelineGraph: data,
  };
};

// GetSourcesReq

const SourcesFetcher = (client: ApiClient) => {
  return async (params: { key: string }) => {
    return await (await (await client)()).getSources(new GetSourcesReq({}));
  };
};

export const useSources = (client: ApiClient) => {
  const { data, isLoading } = useSWR(sourcesKey(), SourcesFetcher(client));

  return {
    sources: data,
    sourcesLoading: isLoading,
  };
};

// GetSinksReq

const SinksFetcher = (client: ApiClient) => {
  return async (params: { key: string }) => {
    return await (await (await client)()).getSinks(new GetSinksReq({}));
  };
};

export const useSinks = (client: ApiClient) => {
  const { data } = useSWR(sinksKey(), SinksFetcher(client));

  let allSinks: Array<SinkOpt> = [
    { name: 'Web', value: { case: 'builtin', value: BuiltinSink.Web } },
    { name: 'Log', value: { case: 'builtin', value: BuiltinSink.Log } },
    { name: 'Null', value: { case: 'builtin', value: BuiltinSink.Null } },
  ];

  if (data) {
    data.sinks.forEach(sink => {
      allSinks.push({
        name: sink.name,
        value: {
          case: 'user',
          value: sink.name,
        },
      });
    });
  }

  return {
    sinks: allSinks,
  };
};

// Connection

export const useConnection = () => {
  // TODO: useSwr call

  const createConnection = async (postConnection: PostConnections) => {
    await post('/v1/connections', { body: postConnection });
  };

  const testConnection = async (postConnection: PostConnections) => {
    await post('/v1/connections/test', { body: postConnection });
  };

  return {
    createConnection,
    testConnection,
  };
};

const ConnectionsFetcher = async () => {
  return await get('/v1/connections', {});
};

export const useConnections = () => {
  const { data } = useSWR(connectionsKey(), ConnectionsFetcher);

  return {
    connections: data?.data,
  };
};
