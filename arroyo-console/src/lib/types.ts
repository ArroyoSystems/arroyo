import { BuiltinSink } from '../gen/api_pb';

export type SqlOptions = {
  name?: string;
  parallelism?: number;
  sink?: number;
  checkpointMS?: number;
};

export type SinkOpt = {
  name: string;
  value:
    | {
        value: BuiltinSink;
        case: 'builtin';
      }
    | {
        value: string;
        case: 'user';
      };
};
