import React from 'react';
import { Pipeline, usePipelineJobs } from '../lib/data_fetching';
import { IconButton, Link, Td, Tr } from '@chakra-ui/react';
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
        {jobs.map(job => (
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
