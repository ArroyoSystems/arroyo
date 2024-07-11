import { Text, Button, Stack, Box, HStack, Spacer, Spinner } from '@chakra-ui/react';
import { useEffect, useRef, useState, useCallback } from 'react';
import { Job, OutputData } from '../../lib/data_fetching';
import { AgGridReact } from 'ag-grid-react';
import 'ag-grid-community/styles/ag-grid.css';
import '@fontsource/ibm-plex-mono';
import '../../styles/data-grid-style.css';

export function PipelineOutputs({
  pipelineId,
  job,
  onDemand = false,
}: {
  pipelineId: string;
  job: Job;
  onDemand: boolean;
}) {
  const gridRef = useRef<AgGridReact>(null);
  const outputSource = useRef<EventSource | undefined>(undefined);
  const currentJobId = useRef<string | undefined>(undefined);
  const [cols, setCols] = useState<any | undefined>(undefined);
  const [rows, _setRows] = useState<any[]>([]);
  const [subscribed, setSubscribed] = useState<boolean>(false);
  const rowsRead = useRef(0);
  const rowsInTable = useRef(0);
  const rowRef = useRef<HTMLSpanElement | null>(null);

  const MAX_ROWS = 10_000;

  const sseHandler = (event: MessageEvent) => {
    const record = JSON.parse(event.data) as OutputData;
    const id = record.startId;
    const batch: any[] = JSON.parse(record.batch);

    if (cols == undefined && batch.length > 0) {
      setCols([
        {
          headerName: '',
          field: 'id',
          width: 70,
          resizable: false,
          pinned: 'left',
          cellStyle: { color: 'var(--chakra-colors-purple-500)' },
        },
        { field: 'timestamp', width: 210, cellStyle: { color: 'var(--chakra-colors-green-500)' } },
        ...Object.keys(batch[0]).map(k => {
          return {
            headerName: k,
            field: k,
          };
        }),
      ]);
    }

    rowsRead.current = rowsRead.current + batch.length;
    rowsInTable.current = rowsInTable.current + batch.length;

    if (rowRef.current) {
      rowRef.current.innerText = String(rowsRead.current);
    }

    batch.forEach((r, i) => {
      Object.keys(r).forEach(k => {
        if (typeof r[k] === 'object') {
          r[k] = JSON.stringify(r[k]);
        }
      });

      r.id = id + i;
      r.timestamp = new Date(record.timestamps[i] / 1000).toISOString();
    });

    batch.reverse();

    let op: any = {
      add: batch,
      addIndex: 0,
    };

    if (rowsInTable.current > MAX_ROWS) {
      let remove: any[] = [];
      gridRef.current!.api.forEachNode((node, idx) => {
        if (idx > rowsInTable.current - MAX_ROWS) {
          remove.push(node.data);
        }
      });
      op.remove = remove;
      rowsInTable.current = rowsInTable.current - remove.length;
    }

    gridRef.current!.api.applyTransaction(op)!;
  };

  const close = () => {
    if (outputSource.current) {
      outputSource.current.removeEventListener('message', sseHandler);
      outputSource.current.close();
    }
  };

  const clearData = useCallback(() => {
    const rowData: any[] = [];
    gridRef.current!.api.forEachNode(function (node) {
      rowData.push(node.data);
    });
    const res = gridRef.current!.api.applyTransaction({
      remove: rowData,
    })!;
  }, []);

  useEffect(() => {
    if ((onDemand && !subscribed) || job.state != 'Running') {
      close();
      return;
    }

    if (outputSource.current?.readyState != EventSource.CLOSED && currentJobId.current == job.id) {
      return;
    }

    close();

    if (pipelineId && job.id) {
      const url = `/api/v1/pipelines/${pipelineId}/jobs/${job.id}/output`;
      const eventSource = new EventSource(url);
      eventSource.onerror = () => {
        setSubscribed(false);
        eventSource.close();
      };
      outputSource.current = eventSource;
      eventSource.addEventListener('message', sseHandler);
      currentJobId.current = job.id;
    }
  }, [job, subscribed]);

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
