import './pipelines.css';

import {
  Alert,
  AlertDescription,
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
import { JobStatus } from '../../gen/api_pb';
import React, { useRef, useState } from 'react';
import { FiCopy, FiTrash2 } from 'react-icons/fi';
import DeleteJobModal from '../../lib/DeleteJobModal';
import { FaRegStopCircle } from 'react-icons/all';
import { useJobs } from '../../lib/data_fetching';
import { ConnectError } from '@bufbuild/connect-web';

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
        return new Intl.DateTimeFormat('en', {
          dateStyle: 'short',
          timeStyle: 'short',
        }).format(new Date(Number(s.startTime) / 1000));
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

function JobsTable() {
  const navigate = useNavigate();

  const { isOpen, onOpen, onClose } = useDisclosure();
  const cancelRef = useRef<HTMLButtonElement>(null);
  const [message, setMessage] = useState<{ text: string; type: 'success' | 'error' } | null>(null);
  const [jobToBeDeleted, setJobToBeDeleted] = useState<JobStatus | null>(null);

  const { jobs, deleteJob: swrDeleteJob, stopJob } = useJobs();

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
      await swrDeleteJob(job.jobId);
      setMessage({ text: `Job ${job.jobId} successfully deleted`, type: 'success' });
    } catch (e) {
      if (e instanceof ConnectError) {
        setMessage({ text: e.rawMessage, type: 'error' });
      } else {
        setMessage({ text: 'Something went wrong', type: 'error' });
      }
    }
  };

  const tableHead = (
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
  );

  const tableBody = (
    <Tbody>
      {jobs?.jobs.flatMap(job => (
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
              icon={<FaRegStopCircle fontSize="1.25rem" />}
              variant="ghost"
              aria-label="Stop"
              title="Stop"
              onClick={() => stopJob(job.jobId)}
              isDisabled={job.state != 'Running'}
            />
            <IconButton
              icon={<FiTrash2 fontSize="1.25rem" />}
              variant="ghost"
              aria-label="Delete source"
              onClick={() => {
                setJobToBeDeleted(job);
                onOpen();
              }}
              title="Delete"
            />
          </Td>
        </Tr>
      ))}
    </Tbody>
  );

  const deleteJobModal = (
    <DeleteJobModal
      isOpen={isOpen}
      cancelRef={cancelRef}
      onClose={onClose}
      jobToBeDeleted={jobToBeDeleted}
      deleteJob={deleteJob}
    />
  );

  return (
    <Stack spacing={2}>
      {deleteJobModal}
      {messageBox}
      <Table>
        {tableHead}
        {tableBody}
      </Table>
    </Stack>
  );
}

export function JobsIndex() {
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
            <JobsTable />
          </Stack>
        </Box>
      </Stack>
    </Container>
  );
}
