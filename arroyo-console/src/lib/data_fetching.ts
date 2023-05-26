import {
  CheckpointDetailsReq,
  JobCheckpointsReq,
  JobDetailsReq,
  JobMetricsReq,
  StopType,
} from '../gen/api_pb';
import { ApiClient } from '../main';
import useSWR from 'swr';
import { mutate as globalMutate } from 'swr';

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
    refreshInterval: 1000,
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
  const { data } = useSWR(checkpointDetailsKey(jobId, epoch), checkpointDetailsFetcher(client));

  return {
    checkpoint: data,
  };
};
