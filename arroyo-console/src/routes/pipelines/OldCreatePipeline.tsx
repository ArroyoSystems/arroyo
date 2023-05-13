import './pipelines.css';

import { Alert, AlertIcon, Box, Button, Flex, FormLabel, Input, Text } from '@chakra-ui/react';
import { FormEvent, useEffect, useRef, useState } from 'react';
import { Form } from 'react-router-dom';
import { BuiltinSink, CreateJobReq, CreatePipelineReq, CreateSourceReq } from '../../gen/api_pb';
import * as monaco from 'monaco-editor/esm/vs/editor/editor.api';
import 'monaco-sql-languages/out/esm/flinksql/flinksql.contribution';
import { ApiClient } from '../../main';

window.MonacoEnvironment = {
  getWorkerUrl: function (moduleId, label) {
    switch (label) {
      case 'flinksql': {
        return './flinksql.worker.js';
      }
      default: {
        return './editor.worker.js';
      }
    }
  },
};

const query5 = `SELECT most_active._0 as count, most_active._1 as auction FROM (
SELECT lexographic_max(count, auction) as most_active, window FROM (
    SELECT auction, count, window FROM (
      SELECT       auction,
    hop(INTERVAL '2' second, INTERVAL '10' second ) as window,
    count(*) as count
  FROM (SELECT bid.auction as auction from nexmark where bid is not null)
  GROUP BY 1, 2)) GROUP BY 2)
`;

export function OldCreatePipeline({ client }: { client: ApiClient }) {
  const [error, setError] = useState<String | null>(null);
  const [editor, setEditor] = useState<monaco.editor.IStandaloneCodeEditor | null>(null);
  const monacoEl = useRef(null);
  const created = useRef(false);

  useEffect(() => {
    if (monacoEl && !editor && !created.current) {
      let e = monaco.editor.create(monacoEl.current!, {
        value: query5,
        language: 'flinksql',
        theme: 'vs-dark',
        minimap: {
          enabled: false,
        },
      });
      created.current = true;
      setEditor(e);
    }

    return () => editor?.dispose();
  }, []);

  async function startPipeline(event: FormEvent<HTMLFormElement>) {
    const parallelism = Number((document.getElementById('parallelism') as HTMLInputElement).value);
    const qps = Number((document.getElementById('nexmark_qps') as HTMLInputElement).value);
    const runtime = Number((document.getElementById('nexmark_time') as HTMLInputElement).value);
    const checkpointMicros =
      BigInt(1000000) *
      BigInt((document.getElementById('checkpoint_frequency') as HTMLInputElement).value);
    let sql = editor?.getValue();

    console.log('Submit', sql, parallelism, qps, runtime);

    let sourceName = `nexmark_${Math.round(new Date().getTime() / 1000)}`;

    let createSource = new CreateSourceReq({
      name: sourceName,
      typeOneof: {
        case: 'nexmark',
        value: {
          eventsPerSecond: qps,
          runtimeMicros: BigInt(runtime * 1000 * 1000),
        },
      },
    });

    await (await client()).createSource(createSource);
    console.log('Created source ' + sourceName);

    let req = new CreatePipelineReq({
      name: 'sql-job',
      config: {
        // @ts-ignore
        case: 'sql',
        // @ts-ignore
        value: {
          // hacky but temporary until the source interface exists
          query: sql?.replace('nexmark', sourceName),
          parallelism: BigInt(parallelism),
          sink: {
            case: 'builtin',
            value: BuiltinSink.Web,
          },
        },
      },
    });

    var createPipelineRes = null;
    try {
      createPipelineRes = await (await client()).createPipeline(req);
      console.log('Result {}', createPipelineRes);
    } catch (e) {
      setError('Compilation failed');
      return;
    }

    console.log('Created pipeline', createPipelineRes.pipelineId);

    let createJob = new CreateJobReq({
      pipelineId: createPipelineRes.pipelineId,
      checkpointIntervalMicros: checkpointMicros,
    });

    try {
      let result = await (await client()).createJob(createJob);
      console.log('Result {}', result);
      window.location.href = '/jobs/' + result.jobId;
    } catch (e) {
      setError('Compilation failed');
    }
  }

  let errorAlert = null;
  if (error != null) {
    errorAlert = (
      <Alert status="error">
        <AlertIcon />
        {error}
      </Alert>
    );
  }

  return (
    <Box>
      <Text fontSize={20}>Create Pipeline</Text>
      <Form onSubmit={startPipeline}>
        <Box id="error" marginTop={5}>
          {errorAlert}
        </Box>

        <Box marginTop={5} height="400px">
          <div className="editor" ref={monacoEl}></div>
        </Box>
        <Flex marginTop={5}>
          <Box>
            <FormLabel>Parallelism</FormLabel>
            <Input type="number" defaultValue={4} width={130} id="parallelism"></Input>
          </Box>
          <Box marginLeft={5}>
            <FormLabel>Runtime (s)</FormLabel>
            <Input type="number" defaultValue={20} width={130} id="nexmark_time"></Input>
          </Box>
          <Box marginLeft={5}>
            <FormLabel>Nexmark QPS</FormLabel>
            <Input type="number" defaultValue={1000} width={130} id="nexmark_qps"></Input>
          </Box>
          <Box marginLeft={5}>
            <FormLabel>Checkpoint interval (s)</FormLabel>
            <Input type="number" defaultValue={60} width={130} id="checkpoint_frequency"></Input>
          </Box>
        </Flex>
        <Box marginTop={5}>
          <Button type="submit">Start Pipeline</Button>
        </Box>
      </Form>
    </Box>
  );
}
