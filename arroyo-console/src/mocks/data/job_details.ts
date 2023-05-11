import { JobDetailsResp, StopType } from '../../gen/api_pb';

export const jobDetailsResponse = new JobDetailsResp({
  jobStatus: {
    jobId: 'abc123',
    pipelineName: 'test_pipeline',
    runningDesired: true,
    state: 'Running',
    runId: 9n,
    definitionId: '12',
    startTime: 1683524279542683n,
    tasks: 4n,
    definition: 'SELECT bid FROM nexmark WHERE bid IS NOT NULL;\n',
    udfs: [{}],
  },
  jobGraph: {
    nodes: [
      {
        nodeId: 'nexmark_0',
        operator: 'nexmark_0:UnboundedNexmarkSource<qps: 100>',
        parallelism: 1,
      },
      {
        nodeId: 'watermark_1',
        operator: 'watermark_1:Watermark',
        parallelism: 1,
      },
      {
        nodeId: 'sink_4',
        operator: 'sink_4:NullSink',
        parallelism: 1,
      },
      {
        nodeId: 'value_project_2',
        operator: 'value_project_2:expression<fused<value_map,filter>:OptionalRecord>',
        parallelism: 1,
      },
    ],
    edges: [
      {
        srcId: 'nexmark_0',
        destId: 'watermark_1',
        keyType: '()',
        valueType: 'arroyo_types::nexmark::Event',
        edgeType: 'Forward',
      },
      {
        srcId: 'value_project_2',
        destId: 'sink_4',
        keyType: '()',
        valueType: 'generated_struct_16429945049069439673',
        edgeType: 'Forward',
      },
      {
        srcId: 'watermark_1',
        destId: 'value_project_2',
        keyType: '()',
        valueType: 'arroyo_types::nexmark::Event',
        edgeType: 'Forward',
      },
    ],
  },
  action: StopType.Checkpoint,
  actionText: 'Stop',
});
