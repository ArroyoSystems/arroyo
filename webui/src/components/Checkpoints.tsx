import React, { useEffect, useState } from 'react';
import {
  Alert,
  AlertDescription,
  AlertIcon,
  AlertTitle,
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
import {
  Checkpoint,
  Job,
  OperatorCheckpointGroup,
  Pipeline,
  useCheckpointDetails,
} from '../lib/data_fetching';
import { dataFormat, formatError } from '../lib/util';
import { CheckpointDetails, formatDuration } from './CheckpointDetails';

export interface CheckpointsProps {
  pipeline: Pipeline;
  job: Job;
  checkpoints: Array<Checkpoint>;
  checkpointsError?: any;
}

const CHECKPOINT_LEADER_UNAVAILABLE_MESSAGE =
  "The leader for this pipeline could not be reached, possibly because it's failed or stopped";

interface CachedCheckpoint {
  checkpoint?: Checkpoint;
  details?: OperatorCheckpointGroup[];
}

const CheckpointErrorAlert: React.FC<{ error: any }> = ({ error }) => {
  const leaderUnavailable = error?.status === 502;

  return (
    <Alert status={leaderUnavailable ? 'warning' : 'error'} marginTop={5}>
      <AlertIcon />
      <Box>
        <AlertTitle>
          {leaderUnavailable
            ? 'Checkpoint information unavailable'
            : 'Failed to load checkpoint information'}
        </AlertTitle>
        <AlertDescription>
          {leaderUnavailable ? CHECKPOINT_LEADER_UNAVAILABLE_MESSAGE : formatError(error)}
        </AlertDescription>
      </Box>
    </Alert>
  );
};

const CheckpointCacheAlert: React.FC<{
  checkpointsError?: any;
  checkpointDetailsError?: any;
  showingCachedCheckpoint: boolean;
}> = ({ checkpointsError, checkpointDetailsError, showingCachedCheckpoint }) => {
  let reason = 'the latest checkpoint information is unavailable';

  if (showingCachedCheckpoint) {
    reason = 'this checkpoint is no longer present in the refreshed checkpoint history';
  } else if (checkpointDetailsError) {
    reason = 'the latest checkpoint details could not be loaded';
  } else if (checkpointsError) {
    reason = 'the checkpoint list could not be refreshed';
  }

  return (
    <Alert status="warning" marginTop={5}>
      <AlertIcon />
      <Box>
        <AlertTitle>Showing cached checkpoint information</AlertTitle>
        <AlertDescription>{`Using the previously loaded data because ${reason}.`}</AlertDescription>
      </Box>
    </Alert>
  );
};

const Checkpoints: React.FC<CheckpointsProps> = ({
  pipeline,
  job,
  checkpoints,
  checkpointsError,
}) => {
  const [epoch, setEpoch] = useState<number | undefined>(undefined);
  const [checkpointCache, setCheckpointCache] = useState<Record<number, CachedCheckpoint>>({});
  const { checkpointDetails, checkpointLoading, checkpointDetailsError } = useCheckpointDetails(
    pipeline.id,
    job.id,
    epoch
  );

  useEffect(() => {
    setEpoch(undefined);
    setCheckpointCache({});
  }, [pipeline.id, job.id]);

  useEffect(() => {
    if (epoch == null) return;

    const checkpoint = checkpoints.find(c => c.epoch == epoch);
    if (!checkpoint) return;

    setCheckpointCache(cache => ({
      ...cache,
      [epoch]: {
        ...cache[epoch],
        checkpoint,
      },
    }));
  }, [epoch, checkpoints]);

  useEffect(() => {
    if (epoch == null || !checkpointDetails) return;

    setCheckpointCache(cache => ({
      ...cache,
      [epoch]: {
        ...cache[epoch],
        details: checkpointDetails,
      },
    }));
  }, [epoch, checkpointDetails]);

  const cachedCheckpoint = epoch == null ? undefined : checkpointCache[epoch];
  const liveCheckpoint = epoch == null ? undefined : checkpoints.find(c => c.epoch == epoch);
  const checkpoint = liveCheckpoint ?? cachedCheckpoint?.checkpoint;
  const selectedCheckpointDetails = checkpointDetails ?? cachedCheckpoint?.details;
  const showingCachedCheckpoint = liveCheckpoint == null && cachedCheckpoint?.checkpoint != null;
  const shouldShowCachedAlert =
    checkpoint != null &&
    selectedCheckpointDetails != null &&
    (checkpointsError || checkpointDetailsError || showingCachedCheckpoint);

  if (checkpointsError && !checkpoint) {
    return <CheckpointErrorAlert error={checkpointsError} />;
  }

  if (!checkpoints.length && !checkpoint) {
    return <Text textStyle="italic">No checkpoints</Text>;
  }

  let details = <Text>Select checkpoint</Text>;

  if (checkpoint) {
    const checkpointHeading = (
      <Heading size="md">
        Checkpoint {epoch}
        <Badge marginLeft={2}>{checkpoint.backend}</Badge>
      </Heading>
    );

    const cachedAlert = shouldShowCachedAlert ? (
      <CheckpointCacheAlert
        checkpointsError={checkpointsError}
        checkpointDetailsError={checkpointDetailsError}
        showingCachedCheckpoint={showingCachedCheckpoint}
      />
    ) : null;

    if (checkpointDetailsError && !selectedCheckpointDetails) {
      details = (
        <Flex flexDirection={'column'} flexGrow={1}>
          {checkpointHeading}
          <CheckpointErrorAlert error={checkpointDetailsError} />
        </Flex>
      );
    } else if (selectedCheckpointDetails) {
      let start = Number(checkpoint.start_time);
      let end = Number(checkpoint.finish_time ?? new Date().getTime() * 1000);

      let checkpointBytes = selectedCheckpointDetails.map(d => d.bytes).reduce((a, b) => a + b, 0);

      const checkpointStats = (
        <StatGroup width={800} border="1px solid #666" borderRadius="5px" marginTop={5} padding={3}>
          <Stat>
            <StatLabel>Started</StatLabel>
            <StatNumber>
              {new Intl.DateTimeFormat('en-us', {
                dateStyle: undefined,
                timeStyle: 'medium',
              }).format(new Date(Number(checkpoint.start_time) / 1000))}
            </StatNumber>
          </Stat>
          <Stat marginLeft={10}>
            <StatLabel>Finished</StatLabel>
            <StatNumber>
              {checkpoint.finish_time != null
                ? new Intl.DateTimeFormat('en-us', {
                    dateStyle: undefined,
                    timeStyle: 'medium',
                  }).format(new Date(Number(checkpoint.finish_time) / 1000))
                : '-'}
            </StatNumber>
          </Stat>
          <Stat marginLeft={10}>
            <StatLabel>Duration</StatLabel>
            <StatNumber>{formatDuration(end - start)}</StatNumber>
          </Stat>
          <Stat marginLeft={10}>
            <StatLabel>Total Size</StatLabel>
            <StatNumber> {dataFormat(checkpointBytes)} </StatNumber>
          </Stat>
        </StatGroup>
      );

      details = (
        <Flex flexDirection={'column'} flexGrow={1}>
          {checkpointHeading}
          {cachedAlert}
          {checkpointStats}
          <CheckpointDetails operators={selectedCheckpointDetails} checkpoint={checkpoint} />
        </Flex>
      );
    } else if (checkpointLoading) {
      details = (
        <Flex flexDirection={'column'} flexGrow={1}>
          {checkpointHeading}
          {cachedAlert}
          <Text marginTop={5}>Loading checkpoint details...</Text>
        </Flex>
      );
    } else {
      details = (
        <Flex flexDirection={'column'} flexGrow={1}>
          {checkpointHeading}
          {cachedAlert}
          <Text marginTop={5}>Select checkpoint</Text>
        </Flex>
      );
    }
  }

  const checkpointMenuItems = checkpoints.slice();
  if (checkpoint && !checkpointMenuItems.some(c => c.epoch == checkpoint.epoch)) {
    checkpointMenuItems.push(checkpoint);
    checkpointMenuItems.sort((a, b) => a.epoch - b.epoch);
  }

  return (
    <Flex>
      <Box w="100px">
        <UnorderedList className="checkpoint-menu">
          {checkpointMenuItems.reverse().map(c => {
            return (
              <ListItem key={'a' + String(c.epoch)} className={c.epoch == epoch ? 'selected' : ''}>
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
