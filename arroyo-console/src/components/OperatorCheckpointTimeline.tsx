import { OperatorCheckpointDetail, TaskCheckpointEventType } from '../gen/api_pb';
import React, { ReactNode, useState } from 'react';
import { Box, Flex, theme } from '@chakra-ui/react';
import * as util from '../lib/util';

export interface OperatorCheckpointTimelineProps {
  op: { id: string; name: string; parallelism: number; checkpoint?: OperatorCheckpointDetail };
  scale: (x: number) => number;
  w: number;
}

const OperatorCheckpointTimeline: React.FC<OperatorCheckpointTimelineProps> = ({
  op,
  scale,
  w,
}) => {
  const [expanded, setExpanded] = useState<boolean>(false);

  let now = new Date().getTime() * 1000;

  let left = scale(Number(op.checkpoint?.startTime));
  let className = op.checkpoint?.finishTime == undefined ? 'in-progress' : 'finished';
  let finishTime = op.checkpoint?.finishTime == undefined ? now : Number(op.checkpoint?.finishTime);
  let width = Math.max(scale(finishTime) - left, 1);

  let tasks = <Box></Box>;

  if (expanded) {
    tasks = (
      <Box marginTop={6} marginLeft={1}>
        {Array(op.parallelism)
          .fill(1)
          .map((x, y) => x + y)
          .map(i => {
            let t = op.checkpoint?.tasks[i - 1];

            let inner: Array<ReactNode> = [];
            if (t != null) {
              let left = scale(Number(t.startTime));

              let alignmentTime = t.events.find(
                e => e.eventType == TaskCheckpointEventType.ALIGNMENT_STARTED
              )?.time;
              let alignmentLeft = scale(Number(alignmentTime));

              let checkpointStarted =
                t.events.find(e => e.eventType == TaskCheckpointEventType.CHECKPOINT_STARTED)
                  ?.time || now;
              let checkpointLeft = scale(Number(checkpointStarted));

              let syncTime = t.events.find(
                e => e.eventType == TaskCheckpointEventType.CHECKPOINT_SYNC_FINISHED
              )?.time;
              let syncLeft = syncTime == null ? null : scale(Number(syncTime));

              let className = Number(t.finishTime) == 0 ? 'in-progress' : 'finished';
              let finishTime = Number(t.finishTime) == 0 ? now : Number(t.finishTime);
              let width = Math.max(scale(finishTime) - left, 1);

              inner = [
                <Box h={1} className={className} position="relative" left={left} width={width} />,
              ];

              inner.push(
                <Box
                  h={1}
                  backgroundColor="orange"
                  position="relative"
                  left={alignmentLeft}
                  width={Math.max((checkpointLeft || scale(now)) - alignmentLeft, 2)}
                />
              );

              if (syncLeft != null) {
                inner.push(
                  <Box
                    h={1}
                    backgroundColor={theme.colors.blue[400]}
                    position="relative"
                    left={checkpointLeft}
                    width={Math.max(syncLeft - checkpointLeft, 2)}
                  />
                );
              }
            }

            return (
              <Box marginTop={2} marginBottom={1} key={i} h={3} w={w} backgroundColor="#333">
                {inner}
              </Box>
            );
          })}
      </Box>
    );
  }

  const timeline = (
    <>
      <Flex
        marginTop="10px"
        textAlign="right"
        className="operator-checkpoint"
        onClick={() => setExpanded(!expanded)}
      >
        <Box width={w + 8} height={10} padding={0} position="relative" backgroundColor="#333">
          <Box
            className={className}
            title={util.durationFormat(finishTime - Number(op.checkpoint?.startTime))}
            position="absolute"
            margin={1}
            height={3}
            left={left}
            width={width}
          ></Box>
          {tasks}
        </Box>
      </Flex>
    </>
  );

  return <>{timeline}</>;
};

export default OperatorCheckpointTimeline;
