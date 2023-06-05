import { CheckpointOverview, JobDetailsResp } from '../gen/api_pb';
import React, { useState } from 'react';
import {
  Badge,
  Box,
  Flex,
  Heading,
  Link,
  ListItem,
  Stat,
  StatGroup,
  StatLabel,
  StatNumber,
  Text,
  UnorderedList,
} from '@chakra-ui/react';
import * as util from '../lib/util';
import { useCheckpointDetails } from '../lib/data_fetching';
import { ApiClient } from '../main';
import CheckpointDetails from './CheckpointDetails';

export interface CheckpointsProps {
  client: ApiClient;
  job: JobDetailsResp;
  checkpoints: Array<CheckpointOverview>;
}

function formatDurationHMS(micros: number): string {
  let millis = micros / 1000;
  let h = Math.floor(millis / 1000 / 60 / 60);
  let m = Math.floor((millis - h * 1000 * 60 * 60) / 1000 / 60);
  let s = Math.floor((millis - h * 1000 * 60 * 60 - m * 1000 * 60) / 1000);

  return `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}:${String(s).padStart(
    2,
    '0'
  )}`;
}

const Checkpoints: React.FC<CheckpointsProps> = ({ client, job, checkpoints }) => {
  const [epoch, setEpoch] = useState<number | undefined>(undefined);
  const { checkpoint, checkpointLoading } = useCheckpointDetails(
    client,
    job.jobStatus?.jobId,
    epoch
  );

  if (!checkpoints.length) {
    return <Text textStyle="italic">No checkpoints</Text>;
  }

  let details = <Text>Select checkpoint</Text>;

  if (checkpoint != null) {
    let start = Number(checkpoint.overview?.startTime);
    let end = Number(
      checkpoint.overview?.finishTime != undefined
        ? checkpoint.overview?.finishTime
        : new Date().getTime() * 1000
    );

    let w = 500;

    function scale(x: number): number {
      return ((x - start) / (end - start)) * w;
    }

    let ops = job.jobGraph?.nodes
      .slice()
      .map(n => {
        return {
          id: n.nodeId,
          name: n.operator,
          parallelism: n.parallelism,
          checkpoint: checkpoint?.operators[n.nodeId],
        };
      })
      .sort((a, b) => Number(a.id.split('_')[1]) - Number(b.id.split('_')[1]));

    let checkpointBytes = Object.values(checkpoint.operators)
      .flatMap(op => Object.values(op.tasks).map(t => (t.bytes == null ? 0 : Number(t.bytes))))
      .reduce((a, b) => a + b, 0);

    const checkpointStats = (
      <StatGroup width={718} border="1px solid #666" borderRadius="5px" marginTop={5} padding={3}>
        <Stat>
          <StatLabel>Started</StatLabel>
          <StatNumber>
            {new Intl.DateTimeFormat('en-us', {
              dateStyle: undefined,
              timeStyle: 'medium',
            }).format(new Date(Number(checkpoint.overview?.startTime) / 1000))}
          </StatNumber>
        </Stat>
        <Stat marginLeft={10}>
          <StatLabel>Finished</StatLabel>
          <StatNumber>
            {checkpoint.overview?.finishTime != null
              ? new Intl.DateTimeFormat('en-us', {
                  dateStyle: undefined,
                  timeStyle: 'medium',
                }).format(new Date(Number(checkpoint.overview?.finishTime) / 1000))
              : '-'}
          </StatNumber>
        </Stat>
        <Stat marginLeft={10}>
          <StatLabel>Duration</StatLabel>
          <StatNumber>{formatDurationHMS(end - start)}</StatNumber>
        </Stat>
        <Stat marginLeft={10}>
          <StatLabel>Total Size</StatLabel>
          <StatNumber> {util.dataFormat(checkpointBytes)} </StatNumber>
        </Stat>
      </StatGroup>
    );

    details = (
      <Flex flexDirection={'column'} flexGrow={1}>
        <Heading size="md">
          Checkpoint {epoch}
          <Badge marginLeft={2}>{checkpoint.overview?.backend}</Badge>
        </Heading>
        {checkpointStats}
        <CheckpointDetails checkpoint={checkpoint} ops={ops} scale={scale} w={w} />
      </Flex>
    );
  }

  return (
    <Flex>
      <Box w="100px">
        <UnorderedList className="checkpoint-menu">
          {checkpoints
            .slice()
            .reverse()
            .map(c => {
              return (
                <ListItem
                  key={'a' + String(c.epoch)}
                  className={c.epoch == epoch ? 'selected' : ''}
                >
                  <Link onClick={() => setEpoch(c.epoch)}> {c.epoch}</Link>
                </ListItem>
              );
            })}
        </UnorderedList>
      </Box>
      <Flex flexGrow={1}>{details}</Flex>
    </Flex>
  );
};

export default Checkpoints;
