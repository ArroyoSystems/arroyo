import { Th, Td, Tr, Table, Thead, Tbody } from '@chakra-ui/react';
import { ReactElement } from 'react';
import { OutputData } from '../../lib/data_fetching';
import { AgGridReact } from 'ag-grid-react';
import 'ag-grid-community/styles/ag-grid.css';
import '@fontsource/ibm-plex-mono';
import '../../styles/data-grid-style.css';

export function PipelineOutputs({ outputs }: { outputs: Array<{ id: number; data: OutputData }> }) {
  let headers: Array<ReactElement> = [];


  const rows = outputs.map(line => {
    let parsed = JSON.parse(line.data.value);
    let object: any = {
      id: line.id,
      timestamp: new Date(Number(line.data.timestamp) / 1000).toISOString(),
    };
    Object.keys(parsed).forEach((k) => {
      object[k.replace(".", "__")] = JSON.stringify(parsed[k])
    });

    return object;
  }).reverse();


  const cols = [
    { headerName: '', field: 'id', width: 40, resizable: false, cellStyle: { color: 'var(--chakra-colors-purple-500)' } },
    { field: 'timestamp', width: 210, cellStyle: { color: 'var(--chakra-colors-green-500)' } },
    ...Object.keys(JSON.parse(outputs[0].data.value)).map(k => {
      return {
        headerName: k,
        field: k.replaceAll('.', '__')
      };
    }),
  ];

  return (
    <div
      style={{ height: "100%" }} // the grid will fill the size of the parent container
    >
      <AgGridReact
        autoSizeStrategy={{ type: 'fitCellContents'}}
        skipHeaderOnAutoSize={true}
       className='ag-theme-custom' rowData={rows} columnDefs={cols} />
    </div>
  );

}
