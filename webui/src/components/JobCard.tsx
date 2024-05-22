import React from 'react';
import { Job } from '../lib/data_fetching';
import { Card, CardBody, Flex } from '@chakra-ui/react';
import Indicator from './Indicator';

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

const JobCard: React.FC<JobCardProps> = ({ job }) => {
  return (
    <Card width={'400px'}>
      <CardBody>
        <Flex justifyContent={'space-between'}>
          <Indicator content={job.state} />
          <Indicator content={formatDuration(jobDuration(job))} label={'Runtime'} />
          <Indicator content={job.tasks ? job.tasks.toString() : 'n/a'} label={'Tasks'} />
        </Flex>
      </CardBody>
    </Card>
  );
};

export default JobCard;
