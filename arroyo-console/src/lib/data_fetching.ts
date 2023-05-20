import {
  CheckpointDetailsReq,
  GetJobsReq,
  JobCheckpointsReq,
  JobDetailsReq,
  JobMetricsReq,
  StopType,
} from '../gen/api_pb';
import { getClient } from './CloudComponents';
import useSWR from 'swr';
import { mutate as globalMutate } from 'swr';

const client = getClient();

// Keys

const jobsKey = () => {
  return { key: 'Job' };
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

// GetJobsReq

const jobsFetcher = async () => {
  return await (await client()).getJobs(new GetJobsReq({}));
};

export const useJobs = () => {
  const { data, mutate } = useSWR(jobsKey(), jobsFetcher, {
    refreshInterval: 1000,
  });

  const deleteJob = async (jobId: string) => {
    await (
      await client()
    ).deleteJob({
      jobId,
    });
    mutate();
  };

  const stopJob = async (jobId: string) => {
    (await client()).updateJob({
      jobId,
      stop: StopType.Graceful,
    });
    mutate();
  };

  return {
    jobs: data,
    deleteJob,
    stopJob,
  };
};

// JobDetailsReq

const jobFetcher = async (params: { jobId: string }) => {
  return await (
    await client()
  ).getJobDetails(
    new JobDetailsReq({
      jobId: params.jobId,
    })
  );
};

export const useJob = (jobId?: string) => {
  const { data, mutate, error } = useSWR(jobDetailsKey(jobId), jobFetcher, {
    refreshInterval: 1000,
  });

  const updateJob = async (params: { parallelism?: number; stop?: StopType }) => {
    await (
      await client()
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

const jobMetricsFetcher = async (params: { jobId: string }) => {
  return await (
    await client()
  ).getJobMetrics(
    new JobMetricsReq({
      jobId: params.jobId,
    })
  );
};

export const useJobMetrics = (jobId?: string) => {
  const { data, isLoading, mutate, isValidating, error } = useSWR(
    jobMetricsKey(jobId),
    jobMetricsFetcher,
    {
      refreshInterval: 5000,
    }
  );

  return {
    metrics: data,
  };
};

// JobCheckpointsReq

const jobCheckpointsFetcher = async (params: { key: string; jobId: string }) => {
  return await (
    await client()
  ).getCheckpoints(
    new JobCheckpointsReq({
      jobId: params.jobId,
    })
  );
};

export const useJobCheckpoints = (jobId?: string) => {
  const { data } = useSWR(jobCheckpointsKey(jobId), jobCheckpointsFetcher, {
    refreshInterval: 5000,
  });

  return {
    checkpoints: data,
  };
};

// CheckpointDetailsReq

const checkpointDetailsFetcher = async (params: { key: string; jobId: string; epoch: number }) => {
  return await (
    await client()
  ).getCheckpointDetail(
    new CheckpointDetailsReq({
      jobId: params.jobId,
      epoch: params.epoch,
    })
  );
};

export const useCheckpointDetails = (jobId?: string, epoch?: number) => {
  const { data } = useSWR(checkpointDetailsKey(jobId, epoch), checkpointDetailsFetcher);

  return {
    checkpoint: data,
  };
};
