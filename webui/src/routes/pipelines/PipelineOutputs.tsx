import { Text, Button, Stack, Box, HStack, Spacer, Spinner } from '@chakra-ui/react';
import { useEffect, useRef, useState, useCallback } from 'react';
import { get, Job, OutputData } from '../../lib/data_fetching';
import { AgGridReact } from 'ag-grid-react';
import 'ag-grid-community/styles/ag-grid.css';
import '@fontsource/ibm-plex-mono';
import '../../styles/data-grid-style.css';

const MAX_ROWS = 10_000;
const REFILL_BUFFER_MS = 150;
const EMPTY_POLL_DELAY_MS = 50;
const TARGET_DRAIN_FRAMES = 30;
const MAX_ROWS_PER_FRAME = 500;

type QueuedRow = {
  data: any;
  timestamp: number;
};

export function PipelineOutputs({
  pipelineId,
  job,
  operatorId,
  onDemand = false,
}: {
  pipelineId: string;
  job: Job;
  operatorId: string;
  onDemand: boolean;
}) {
  const gridRef = useRef<AgGridReact>(null);
  const nextOffset = useRef(0);
  const currentOutput = useRef<string | undefined>(undefined);
  const lastTimestamp = useRef<number | undefined>(undefined);
  const queuedRows = useRef<QueuedRow[]>([]);
  const pollGeneration = useRef(0);
  const rendererGeneration = useRef<number | undefined>(undefined);
  const jobState = useRef(job.state);
  jobState.current = job.state;
  const [cols, setCols] = useState<any | undefined>(undefined);
  const [rows, _setRows] = useState<any[]>([]);
  const [subscribed, setSubscribed] = useState<boolean>(false);
  const rowsRead = useRef(0);
  const rowsInTable = useRef(0);
  const rowRef = useRef<HTMLSpanElement | null>(null);

  const enqueueOutput = useCallback((record: OutputData) => {
    const id = record.start_id;
    const batch = record.batch as any[];

    if (batch.length > 0) {
      setCols(
        (current: any | undefined) =>
          current ?? [
            {
              headerName: '',
              field: 'id',
              width: 70,
              resizable: false,
              pinned: 'left',
              cellStyle: { color: 'var(--chakra-colors-purple-500)' },
            },
            {
              field: 'timestamp',
              width: 210,
              cellStyle: { color: 'var(--chakra-colors-green-500)' },
            },
            ...Object.keys(batch[0]).map(k => ({
              headerName: k,
              field: k,
            })),
          ]
      );
    }

    for (const [i, row] of batch.entries()) {
      const timestamp = record.timestamps[i];
      Object.keys(row).forEach(k => {
        if (typeof row[k] === 'object') {
          row[k] = JSON.stringify(row[k]);
        }
      });

      row.id = id + i;
      row.timestamp = new Date(timestamp / 1000).toISOString();
      queuedRows.current.push({ data: row, timestamp });
    }

    return batch.length;
  }, []);

  const renderRows = useCallback((rows: any[]) => {
    rowsRead.current += rows.length;
    rowsInTable.current += rows.length;
    if (rowRef.current) {
      rowRef.current.innerText = String(rowsRead.current);
    }

    const api = gridRef.current!.api;
    api.applyTransaction({ add: [...rows].reverse(), addIndex: 0 })!;

    if (rowsInTable.current > MAX_ROWS) {
      const remove: any[] = [];
      api.forEachNode((node, idx) => {
        if (idx >= MAX_ROWS) {
          remove.push(node.data);
        }
      });
      api.applyTransaction({ remove })!;
      rowsInTable.current -= remove.length;
    }
  }, []);

  const clearData = useCallback(() => {
    const rowData: any[] = [];
    gridRef.current!.api.forEachNode(function (node) {
      rowData.push(node.data);
    });
    gridRef.current!.api.applyTransaction({
      remove: rowData,
    })!;
    rowsInTable.current = 0;
  }, []);

  useEffect(() => {
    const generation = ++pollGeneration.current;
    const outputKey = `${job.id}:${operatorId}`;
    if (currentOutput.current !== outputKey) {
      currentOutput.current = outputKey;
      nextOffset.current = 0;
      lastTimestamp.current = undefined;
      queuedRows.current = [];
    }

    if (onDemand && !subscribed) {
      return;
    }

    let cancelled = false;

    const wait = (delay: number) => new Promise<void>(resolve => window.setTimeout(resolve, delay));

    const waitForFrame = (delay: number) =>
      new Promise<void>(resolve => {
        window.setTimeout(() => window.requestAnimationFrame(() => resolve()), delay);
      });

    const render = async () => {
      if (rendererGeneration.current === generation || queuedRows.current.length === 0) {
        return;
      }

      rendererGeneration.current = generation;
      await wait(REFILL_BUFFER_MS);
      let firstRow = true;

      while (pollGeneration.current === generation && queuedRows.current.length > 0) {
        const rowsThisFrame = Math.min(
          MAX_ROWS_PER_FRAME,
          Math.max(1, Math.ceil(queuedRows.current.length / TARGET_DRAIN_FRAMES))
        );
        const row = queuedRows.current[0];
        const timestampDelay =
          lastTimestamp.current === undefined ? 0 : (row.timestamp - lastTimestamp.current) / 1000;
        const renderDelay =
          firstRow || rowsThisFrame > 1 ? 0 : Math.min(250, Math.max(16, timestampDelay));
        await waitForFrame(renderDelay);

        if (pollGeneration.current !== generation) {
          break;
        }

        const rows = queuedRows.current.splice(0, rowsThisFrame);
        lastTimestamp.current = rows[rows.length - 1].timestamp;
        renderRows(rows.map(row => row.data));
        firstRow = false;
      }

      if (rendererGeneration.current === generation) {
        rendererGeneration.current = undefined;
      }
    };

    const poll = async () => {
      while (!cancelled) {
        const { data, error } = await get('/v1/pipelines/{pipeline_id}/jobs/{job_id}/output', {
          params: {
            path: { pipeline_id: pipelineId, job_id: job.id },
            query: { operator_id: operatorId, offset: nextOffset.current },
          },
        });

        if (cancelled) {
          return;
        }

        if (error) {
          console.error('Failed to read preview output', error);
          if (onDemand) {
            setSubscribed(false);
          }
          return;
        }

        let receivedRows = 0;
        for (const record of data ?? []) {
          const rows = enqueueOutput(record);
          receivedRows += rows;
          if (currentOutput.current === outputKey) {
            nextOffset.current = Math.max(nextOffset.current, record.start_id + rows);
          }
        }
        render();

        if (['Finished', 'Failed', 'Stopped'].includes(jobState.current)) {
          return;
        }
        if (receivedRows === 0) {
          await wait(EMPTY_POLL_DELAY_MS);
        }
      }
    };

    render();
    poll();

    return () => {
      cancelled = true;
      if (pollGeneration.current === generation) {
        pollGeneration.current++;
      }
    };
  }, [enqueueOutput, job.id, onDemand, operatorId, pipelineId, renderRows, subscribed]);

  return (
    <Stack h="100%">
      <Box h="100%" display={onDemand && !subscribed && !cols ? 'none' : 'block'}>
        <AgGridReact
          autoSizeStrategy={{ type: 'fitCellContents' }}
          skipHeaderOnAutoSize={true}
          ref={gridRef}
          suppressFieldDotNotation={true}
          animateRows={false}
          className="ag-theme-custom"
          rowData={rows}
          columnDefs={cols}
          suppressDragLeaveHidesColumns={true}
          enableCellTextSelection={true}
        />
      </Box>
      {onDemand && (
        <HStack spacing={4}>
          <Button
            onClick={() => setSubscribed(!subscribed)}
            size="xs"
            opacity={'0.9'}
            colorScheme={!subscribed ? 'green' : 'gray'}
            variant={'solid'}
            rounded={'sm'}
            isLoading={job.state != 'Running'}
            title={
              subscribed
                ? 'Stop tailing output'
                : job.state == 'Running'
                ? 'Start tailing output from pipeline'
                : 'Job must be running to tail output'
            }
          >
            <HStack spacing={2}>
              {subscribed && <Spinner size="xs" speed={'0.9s'} />}
              <Text>{!subscribed ? 'Start tailing' : 'Stop tailing'}</Text>
            </HStack>
          </Button>
          <Button rounded={'sm'} size="xs" onClick={clearData} variant={'outline'}>
            Clear
          </Button>
          <Spacer />
          <Text fontFamily={'monospace'}>
            read <span ref={rowRef}>{rowsRead.current}</span> rows
          </Text>
        </HStack>
      )}
    </Stack>
  );
}
