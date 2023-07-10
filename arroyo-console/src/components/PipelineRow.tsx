import React from 'react';
import { Pipeline, usePipelineJobs } from '../lib/data_fetching';
import { Td, Tr } from '@chakra-ui/react';
import { Link } from 'react-router-dom';
import { JobStatus } from '../gen/api_pb';

export interface PipelineRowProps {
  pipeline: Pipeline;
}

interface ColumnDef {
  name: string;
  accessor: (p: Pipeline) => string;
}

function pipelineDuration(pipeline: JobStatus) {
  if (pipeline.startTime == null) {
    return 0;
  } else if (pipeline.finishTime == null) {
    return Date.now() * 1000 - Number(pipeline.startTime);
  } else {
    return pipeline.finishTime - pipeline.startTime;
  }
}

function formatDuration(micros: number): string {
  let millis = micros / 1000;
  let secs = Math.floor(millis / 1000);
  if (millis < 1000) {
    return `${millis}ms`;
  } else if (secs < 60) {
    return `${secs}s`;
  } else if (millis / 1000 < 60 * 60) {
    let minutes = Math.floor(secs / 60);
    let seconds = secs - minutes * 60;
    return `${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
  } else {
    let hours = Math.floor(secs / (60 * 60));
    let minutes = Math.floor((secs - hours * 60 * 60) / 60);
    let seconds = secs - hours * 60 * 60 - minutes * 60;
    return `${hours}:${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
  }
}

const PipelineRow: React.FC<PipelineRowProps> = ({ pipeline }) => {
  const { jobs } = usePipelineJobs(pipeline.id);

  return (
    <Tr key={pipeline.id}>
      <Td key={'id'}>
        <Link to={`/jobs/${pipeline.id}`}>{pipeline.id}</Link>
      </Td>
      <Td key={'name'}>{pipeline.name}</Td>
      <Td key={'job'}>{JSON.stringify(jobs)}</Td>
      {/* <Td textAlign="right"> */}
      {/*   <IconButton */}
      {/*     onClick={() => navigate('/pipelines/new?from=' + pipeline.definitionId)} */}
      {/*     icon={<FiCopy fontSize="1.25rem" />} */}
      {/*     variant="ghost" */}
      {/*     aria-label="Duplicate" */}
      {/*     title="Copy" */}
      {/*   /> */}
      {/*   <IconButton */}
      {/*     icon={<FiXCircle fontSize="1.25rem" />} */}
      {/*     variant="ghost" */}
      {/*     aria-label="Delete source" */}
      {/*     onClick={() => { */}
      {/*       setJobToBeDelete(pipeline); */}
      {/*       onOpen(); */}
      {/*     }} */}
      {/*     title="Delete" */}
      {/*   /> */}
      {/* </Td> */}
    </Tr>
  );
};

export default PipelineRow;
