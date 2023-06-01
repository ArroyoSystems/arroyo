import './pipelines.css';

import { ConnectError } from '@bufbuild/connect-web';
import {
  Alert,
  AlertDescription,
  AlertDialog,
  AlertDialogBody,
  AlertDialogContent,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogOverlay,
  AlertIcon,
  Box,
  Button,
  CloseButton,
  Container,
  Heading,
  HStack,
  IconButton,
  Stack,
  Table,
  Tbody,
  Td,
  Text,
  Th,
  Thead,
  Tr,
  useColorModeValue,
  useDisclosure,
} from '@chakra-ui/react';
import { Link, useLinkClickHandler, useNavigate } from 'react-router-dom';
import { DeleteJobReq, GetJobsReq, JobStatus } from '../../gen/api_pb';
import React, { useEffect, useRef, useState } from 'react';
import { FiCopy, FiXCircle } from 'react-icons/fi';
import { ApiClient } from '../../main';
import { formatDate } from '../../lib/util';

interface ColumnDef {
  name: string;
  accessor: (s: JobStatus) => string;
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

const columns: Array<ColumnDef> = [
  {
    name: 'name',
    accessor: s => s.pipelineName,
  },
  {
    name: 'state',
    accessor: s => s.state,
  },
  {
    name: 'started',
    accessor: s => {
      if (s.startTime == null) {
        return '-';
      } else {
        return formatDate(s.startTime);
      }
    },
  },
  {
    name: 'runtime',
    accessor: s => formatDuration(Number(pipelineDuration(s))),
  },
  {
    name: 'tasks',
    accessor: s => String(s.tasks ? Number(s.tasks) : '-'),
  },
];

function JobsTable({ client }: { client: ApiClient }) {
  const navigate = useNavigate();
  const [state, setState] = useState<Array<JobStatus> | null>();

  const { isOpen, onOpen, onClose } = useDisclosure();
  const cancelRef = useRef<HTMLButtonElement>(null);
  const [message, setMessage] = useState<{ text: string; type: 'success' | 'error' } | null>(null);
  const [jobToBeDeleted, setJobToBeDelete] = useState<JobStatus | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      const jobs = await (await client()).getJobs(new GetJobsReq({}));

      setState(jobs.jobs);
    };

    fetchData();
  }, [message]);

  let messageBox = null;
  if (message != null) {
    messageBox = (
      <Alert status={message.type} width="100%">
        <AlertIcon />
        <AlertDescription flexGrow={1}>{message.text}</AlertDescription>
        <CloseButton alignSelf="flex-end" right={-1} top={-1} onClick={() => setMessage(null)} />
      </Alert>
    );
  }

  const deleteJob = async (job: JobStatus) => {
    onClose();
    try {
      await (
        await client()
      ).deleteJob(
        new DeleteJobReq({
          jobId: job.jobId,
        })
      );
      setMessage({ text: `Job ${job.jobId} successfully deleted`, type: 'success' });
    } catch (e) {
      if (e instanceof ConnectError) {
        setMessage({ text: e.rawMessage, type: 'error' });
      } else {
        setMessage({ text: 'Something went wrong', type: 'error' });
      }
    }
  };

  return (
    <Stack spacing={2}>
      <AlertDialog isOpen={isOpen} leastDestructiveRef={cancelRef} onClose={onClose}>
        <AlertDialogOverlay>
          <AlertDialogContent>
            <AlertDialogHeader fontSize="lg" fontWeight="bold">
              Delete Job {jobToBeDeleted?.pipelineName}
            </AlertDialogHeader>

            <AlertDialogBody>
              Are you sure you want to delete job {jobToBeDeleted?.jobId}? Job state will be lost.
            </AlertDialogBody>

            <AlertDialogFooter>
              <Button ref={cancelRef} onClick={onClose}>
                Cancel
              </Button>
              <Button colorScheme="red" onClick={() => deleteJob(jobToBeDeleted!)} ml={3}>
                Delete
              </Button>
            </AlertDialogFooter>
          </AlertDialogContent>
        </AlertDialogOverlay>
      </AlertDialog>

      {messageBox}
      <Table>
        <Thead>
          <Tr>
            <Th>Id</Th>
            {columns.map(c => {
              return (
                <Th key={c.name}>
                  <Text>{c.name}</Text>
                </Th>
              );
            })}
            <Th></Th>
          </Tr>
        </Thead>
        <Tbody>
          {state?.flatMap(job => (
            <Tr key={job.jobId}>
              <Td>
                <Link to={`/jobs/${job.jobId}`}>{job.jobId}</Link>
              </Td>
              {columns.map(column => (
                <Td key={job.jobId + column.name}>{column.accessor(job)}</Td>
              ))}
              <Td textAlign="right">
                <IconButton
                  onClick={() => navigate('/pipelines/new?from=' + job.definitionId)}
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
                    setJobToBeDelete(job);
                    onOpen();
                  }}
                  title="Delete"
                />
              </Td>
            </Tr>
          ))}
        </Tbody>
      </Table>
    </Stack>
  );
}

export function JobsIndex({ client }: { client: ApiClient }) {
  return (
    <Container py="8" flex="1">
      <Stack spacing={{ base: '8', lg: '6' }}>
        <Stack
          spacing="4"
          direction={{ base: 'column', lg: 'row' }}
          justify="space-between"
          align={{ base: 'start', lg: 'center' }}
        >
          <Stack spacing="1">
            <Heading size="sm" fontWeight="medium">
              Pipelines
            </Heading>
          </Stack>
          <HStack spacing="3">
            <Button variant="primary" onClick={useLinkClickHandler('/pipelines/new')}>
              Create Pipeline
            </Button>
          </HStack>
        </Stack>
        <Box
          bg="bg-surface"
          boxShadow={{ base: 'none', md: useColorModeValue('sm', 'sm-dark') }}
          borderRadius="lg"
        >
          <Stack spacing={{ base: '5', lg: '6' }}>
            <JobsTable client={client} />
          </Stack>
        </Box>
      </Stack>
    </Container>
  );
}
