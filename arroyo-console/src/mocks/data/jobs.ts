import { GetJobsResp, UdfLanguage } from '../../gen/api_pb';

export const getJobsResp: GetJobsResp = new GetJobsResp({
  jobs: [
    {
      udfs: [
        {
          language: UdfLanguage.Rust,
          definition: '',
        },
      ],
      jobId: 'abc123',
      pipelineName: 'test_pipeline',
      state: 'Running',
      runningDesired: true,
      definitionId: '12',
      runId: 9n,
      startTime: 1683524279542683n,
      tasks: 4n,
      definition: 'SELECT bid FROM nexmark WHERE bid IS NOT NULL;\n',
    },
  ],
});
