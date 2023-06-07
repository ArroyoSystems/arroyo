import { rest } from 'msw';
import { createGrpcResponse, grpcRes } from './util';
import { jobDetailsResponse } from './data/job_details';
import { getJobsResp } from './data/jobs';
import { jobMetricsResponse } from './data/job_metrics';

const getJobsHandler = rest.post('http://localhost:8001/arroyo_api.ApiGrpc/GetJobs', (req, res) => {
  const message = createGrpcResponse(getJobsResp);
  return res(grpcRes(message));
});

const getJobDetailsHandler = rest.post(
  'http://localhost:8001/arroyo_api.ApiGrpc/GetJobDetails',
  async (req, res) => {
    const message = createGrpcResponse(jobDetailsResponse);
    return res(grpcRes(message));
  }
);

const getJobMetricsHandler = rest.post(
  'http://localhost:8001/arroyo_api.ApiGrpc/GetJobMetrics',
  async (req, res) => {
    const message = createGrpcResponse(jobMetricsResponse);
    return res(grpcRes(message));
  }
);

export const handlers = [getJobsHandler, getJobDetailsHandler, getJobMetricsHandler];
