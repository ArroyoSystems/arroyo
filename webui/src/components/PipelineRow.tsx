import React from 'react';
import { Job, Pipeline, usePipelineJobs } from '../lib/data_fetching';
import { Card, CardBody, Flex, IconButton, Link, Td, Tr, Text } from '@chakra-ui/react';
import { formatDate, relativeTime } from '../lib/util';
import { FiCopy, FiXCircle } from 'react-icons/fi';
import { useNavigate } from 'react-router-dom';


export interface IndicatorProps {
  content: string | undefined;
  label?: string;
  color?: string;
}

const Indicator: React.FC<IndicatorProps> = ({ content, label, color }) => {
  return (
    <Flex direction={'column'} justifyContent={'center'} flex={'0 0 100px'}>
      <Text
        as="b"
        fontSize="lg"
        textOverflow={'ellipsis'}
        whiteSpace={'normal'}
        wordBreak={'break-all'}
        noOfLines={1}
        color={color}
      >
        {content}
      </Text>
      {label && <Text fontSize="xs">{label}</Text>}
    </Flex>
  );
};


export interface JobCardProps {
  job: Job;
}

const jobDuration = (job: Job) => {
  if (job.startTime == null) {
    return 0;
  } else if (job.finishTime == null) {
    return Date.now() * 1000 - Number(job.startTime);
  } else {
    return job.finishTime - job.startTime;
  }
};

function stateColor(state: string): string {
  switch (state) {
    case 'Running':
      return 'green.300';
    case 'Failed':
      return 'red.300';
    case 'Stopping':
      return 'orange.300';
  }
  return 'gray.400';
}


function formatDuration(micros: number): string {
  let millis = micros / 1000;
  let secs = Math.floor(millis / 1000);
  if (secs < 60) {
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

  let job = jobs[0];

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
      <Td key={'state'}>
        <Indicator content={job?.state} color={stateColor(job?.state)} />
      </Td>
      <Td>
        <Indicator content={job ? formatDuration(jobDuration(job)) : undefined} />
      </Td>
      <Td>
        <Indicator content={job?.tasks ? job.tasks.toString() : 'n/a'} />
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
