import {
  JobMetricsResp,
  JobMetricsResp_OperatorMetrics,
  Metric,
  SubtaskMetrics,
} from '../../gen/api_pb';

const metric_0 = new Metric({
  time: 1683780375000000n,
  value: 0,
});

const metric_90 = new Metric({
  time: 1683780375000000n,
  value: 90,
});

const subtaskMetrics = new SubtaskMetrics({
  bytesRecv: [metric_0],
  bytesSent: [metric_0],
  messagesRecv: [metric_0],
  messagesSent: [metric_0],
  backpressure: [metric_0],
});

const backpressuredSubtaskMetrics = new SubtaskMetrics({
  bytesRecv: [metric_0],
  bytesSent: [metric_0],
  messagesRecv: [metric_0],
  messagesSent: [metric_0],
  backpressure: [metric_90],
});

const operatorMetrics = new JobMetricsResp_OperatorMetrics({
  subtasks: { 0: subtaskMetrics },
});

const backpressuredOperatorMetrics = new JobMetricsResp_OperatorMetrics({
  subtasks: { 0: backpressuredSubtaskMetrics },
});

export const jobMetricsResponse = new JobMetricsResp({
  metrics: {
    nexmark_0: backpressuredOperatorMetrics,
    watermark_1: backpressuredOperatorMetrics,
    value_project_2: operatorMetrics,
    sink_4: operatorMetrics,
  },
  jobId: 'abc123',
  startTime: 168378035500n,
  endTime: 1683780655000n,
});
