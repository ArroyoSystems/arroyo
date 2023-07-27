import { useNavigate } from 'react-router-dom';
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
  Button,
  CloseButton,
  Stack,
  Table,
  Tbody,
  Text,
  Th,
  Thead,
  Tr,
  useDisclosure,
} from '@chakra-ui/react';
import React, { useRef, useState } from 'react';
import { usePipelines, usePipeline, Pipeline } from '../lib/data_fetching';
import PipelineRow from './PipelineRow';
import { formatError } from '../lib/util';

export interface JobsTableProps {}

const columns = ['Created at', 'Job'];

const PipelinesTable: React.FC<JobsTableProps> = ({}) => {
  const navigate = useNavigate();
  // const [state, setState] = useState<Array<JobStatus> | null>();

  const { isOpen, onOpen, onClose } = useDisclosure();
  const cancelRef = useRef<HTMLButtonElement>(null);
  const [message, setMessage] = useState<{ text: string; type: 'success' | 'error' } | null>(null);
  const [pipelineIdToBeDeleted, setPipelineIdToBeDeleted] = useState<string | undefined>(undefined);
  const { pipeline: pipelineToBeDeleted, deletePipeline } = usePipeline(pipelineIdToBeDeleted);
  const { pipelines, piplinesError } = usePipelines();

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

  if (piplinesError) {
    messageBox = (
      <Alert width="100%">
        <AlertIcon />
        <AlertDescription flexGrow={1}>{formatError(piplinesError)}</AlertDescription>
        <CloseButton alignSelf="flex-end" right={-1} top={-1} onClick={() => setMessage(null)} />
      </Alert>
    );
  }

  const handleDeletePipeline = async () => {
    onClose();
    const { error } = await deletePipeline();

    if (error) {
      setMessage({ text: formatError(error), type: 'error' });
    } else {
      setMessage({
        text: `Pipeline ${pipelineToBeDeleted?.name} successfully deleted`,
        type: 'success',
      });
    }
  };

  const alert = (
    <AlertDialog isOpen={isOpen} leastDestructiveRef={cancelRef} onClose={onClose}>
      <AlertDialogOverlay>
        <AlertDialogContent>
          <AlertDialogHeader fontSize="lg" fontWeight="bold">
            Delete Pipeline {pipelineIdToBeDeleted}
          </AlertDialogHeader>

          <AlertDialogBody>
            Are you sure you want to delete pipeline {pipelineIdToBeDeleted}? Job state will be
            lost.
          </AlertDialogBody>

          <AlertDialogFooter>
            <Button ref={cancelRef} onClick={onClose}>
              Cancel
            </Button>
            <Button colorScheme="red" onClick={() => handleDeletePipeline()} ml={3}>
              Delete
            </Button>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialogOverlay>
    </AlertDialog>
  );

  const tableHeader = (
    <Thead>
      <Tr>
        <Th>Name / ID</Th>
        {columns.map(c => {
          return (
            <Th key={c}>
              <Text>{c}</Text>
            </Th>
          );
        })}
        <Th></Th>
      </Tr>
    </Thead>
  );

  const tableBody = (
    <Tbody>
      {(pipelines as Pipeline[])?.flatMap(pipeline => (
        <PipelineRow
          key={pipeline.id}
          pipeline={pipeline}
          setPipelineIdToBeDeleted={setPipelineIdToBeDeleted}
          onOpen={onOpen}
        />
      ))}
    </Tbody>
  );

  return (
    <Stack>
      {alert}
      {messageBox}
      <Table>
        {tableHeader}
        {tableBody}
      </Table>
    </Stack>
  );
};

export default PipelinesTable;
