import {
  CheckpointDetailsReq,
  CreateUdf,
  GetConnectionTablesReq,
  GetConnectionsReq,
  GetConnectorsReq,
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
  return async (params: { connectionId: string }) => {
    return (await (await client()).getConnectionTables(new GetConnectionTablesReq({}))).tables;
  };
};

export const useConnectionTables = (client: ApiClient) => {
  const { data, isLoading } = useSWR(connectionTablesKey(), connectionTablesFetcher(client), {
    refreshInterval: 5000,
  });

  return {
    connectionTables: data,
    connectionTablesLoading: isLoading,
  };
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
