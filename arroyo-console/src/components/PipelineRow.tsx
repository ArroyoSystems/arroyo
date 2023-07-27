import React from 'react';
import { Job, Pipeline, usePipelineJobs } from '../lib/data_fetching';
import { IconButton, Link, Td, Tr } from '@chakra-ui/react';
import { JobStatus } from '../gen/api_pb';
import JobCard from './JobCard';
import { formatDate, relativeTime } from '../lib/util';
import Indicator from './Indicator';
import { FiCopy, FiXCircle } from 'react-icons/fi';
import { useNavigate } from 'react-router-dom';

export interface PipelineRowProps {
  pipeline: Pipeline;
  setPipelineIdToBeDeleted: (pipelineId: string) => void;
  onOpen: () => void;
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

const PipelineRow: React.FC<PipelineRowProps> = ({
  pipeline,
  setPipelineIdToBeDeleted,
  onOpen,
}) => {
  const { jobs } = usePipelineJobs(pipeline.id);
  let navigate = useNavigate();

  if (!jobs) {
    return <></>;
  }

  return (
    <Tr key={pipeline.id}>
      <Td key={'name'} minWidth={230} maxWidth={'400px'}>
        <Link href={`/pipelines/${pipeline.id}`}>
          <Indicator content={pipeline.name} label={pipeline.id} />
        </Link>
      </Td>
      <Td key={'created_at'} minWidth={230}>
        <Indicator
          content={formatDate(BigInt(pipeline.createdAt))}
          label={relativeTime(pipeline.createdAt)}
        />
      </Td>
      <Td key={'job'}>
        {(jobs as Job[]).map(job => (
          <JobCard key={job.id} job={job} />
        ))}
      </Td>
      <Td key={'actions'} textAlign="right">
        <IconButton
          onClick={() => navigate('/pipelines/new?from=' + pipeline.id)}
          icon={<FiCopy fontSize="1.25rem" />}
          variant="ghost"
          aria-label="Duplicate"
          title="Copy"
        />
        <IconButton
          icon={<FiXCircle fontSize="1.25rem" />}
          variant="ghost"
          aria-label="Delete source"
          onClick={() => {
            setPipelineIdToBeDeleted(pipeline.id);
            onOpen();
          }}
          title="Delete"
        />
      </Td>
    </Tr>
  );
};

export default PipelineRow;
