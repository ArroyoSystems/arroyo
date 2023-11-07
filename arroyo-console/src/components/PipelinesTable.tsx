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
  Th,
  Thead,
  Tr,
  useDisclosure,
} from '@chakra-ui/react';
import React, { useRef, useState } from 'react';
import { usePipelines, usePipeline, Pipeline } from '../lib/data_fetching';
import { formatError } from '../lib/util';
import PipelineRow from './PipelineRow';
import PaginatedContent from './PaginatedContent';

export interface JobsTableProps {}

const PipelinesTable: React.FC<JobsTableProps> = ({}) => {
  const {
    isOpen: deletePipelineModalIsOpen,
    onOpen: deletePipelineModalOnOpen,
    onClose: deletePipelineModalOnClose,
  } = useDisclosure();
  const cancelRef = useRef<HTMLButtonElement>(null);
  const [message, setMessage] = useState<{ text: string; type: 'success' | 'error' } | null>(null);
  const [pipelineIdToBeDeleted, setPipelineIdToBeDeleted] = useState<string | undefined>(undefined);
  const { pipeline: pipelineToBeDeleted, deletePipeline } = usePipeline(pipelineIdToBeDeleted);
  const { pipelinePages, piplinesError, pipelineTotalPages, setPipelinesMaxPages } = usePipelines();
  const [pipelines, setPipelines] = useState<Pipeline[]>([]);

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
    deletePipelineModalOnClose();
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
    <AlertDialog
      isOpen={deletePipelineModalIsOpen}
      leastDestructiveRef={cancelRef}
      onClose={deletePipelineModalOnClose}
    >
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
            <Button ref={cancelRef} onClick={deletePipelineModalOnClose}>
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
        <Th>Created at</Th>
        <Th>Job</Th>
        <Th></Th>
      </Tr>
    </Thead>
  );

  const tableBody = (
    <Tbody>
      {pipelines.map(pipeline => (
        <PipelineRow
          key={pipeline.id}
          pipeline={pipeline}
          setPipelineIdToBeDeleted={setPipelineIdToBeDeleted}
          onOpen={deletePipelineModalOnOpen}
        />
      ))}
    </Tbody>
  );

  const table = (
    <Table>
      {tableHeader}
      {tableBody}
    </Table>
  );

  return (
    <Stack
      spacing={{
        base: '8',
        lg: '6',
      }}
    >
      {alert}
      {messageBox}
      <PaginatedContent
        pages={pipelinePages}
        totalPages={pipelineTotalPages}
        setMaxPages={setPipelinesMaxPages}
        content={table}
        setCurrentData={setPipelines}
      />
    </Stack>
  );
};

export default PipelinesTable;
