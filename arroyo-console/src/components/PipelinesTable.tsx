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
  Flex,
  IconButton,
  Stack,
  Table,
  Tbody,
  Text,
  Th,
  Thead,
  Tr,
  useColorModeValue,
  useDisclosure,
} from '@chakra-ui/react';
import React, { useRef, useState } from 'react';
import { usePipelines, usePipeline } from '../lib/data_fetching';
import { formatError } from '../lib/util';
import { ArrowBackIcon, ArrowForwardIcon } from '@chakra-ui/icons';
import PipelineRow from './PipelineRow';
import Loading from './Loading';

export interface JobsTableProps {}

const columns = ['Created at', 'Job'];

const PipelinesTable: React.FC<JobsTableProps> = ({}) => {
  const { isOpen, onOpen, onClose } = useDisclosure();
  const cancelRef = useRef<HTMLButtonElement>(null);
  const [message, setMessage] = useState<{ text: string; type: 'success' | 'error' } | null>(null);
  const [pipelineIdToBeDeleted, setPipelineIdToBeDeleted] = useState<string | undefined>(undefined);
  const { pipeline: pipelineToBeDeleted, deletePipeline } = usePipeline(pipelineIdToBeDeleted);
  const { pipelinePages, piplinesError, pipelineTotalPages, setPipelinesMaxPages } = usePipelines();
  const [pageNum, setPageNum] = useState<number>(1);

  setPipelinesMaxPages(Math.max(pageNum, pipelineTotalPages));

  if (!pipelinePages || pipelinePages.length != pipelineTotalPages) {
    return <Loading />;
  }

  const currentPage = pipelinePages[pageNum - 1];
  const pipelines = currentPage.data;

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
      {pipelines.map(pipeline => (
        <PipelineRow
          key={pipeline.id}
          pipeline={pipeline}
          setPipelineIdToBeDeleted={setPipelineIdToBeDeleted}
          onOpen={onOpen}
        />
      ))}
    </Tbody>
  );

  const pageButtons = (
    <Flex justifyContent={'center'} gap={'5px'}>
      <IconButton
        aria-label="Previous page"
        icon={<ArrowBackIcon />}
        isDisabled={pageNum === 1}
        onClick={() => setPageNum(pageNum - 1)}
      />
      <IconButton
        aria-label="Search database"
        icon={<ArrowForwardIcon />}
        isDisabled={!currentPage.hasMore}
        onClick={() => setPageNum(pageNum + 1)}
      />
    </Flex>
  );

  return (
    <Stack
      spacing={{
        base: '8',
        lg: '6',
      }}
    >
      <Box
        bg="bg-surface"
        boxShadow={{ base: 'none', md: useColorModeValue('sm', 'sm-dark') }}
        borderRadius="lg"
      >
        {alert}
        {messageBox}
        <Table>
          {tableHeader}
          {tableBody}
        </Table>
      </Box>
      {pageButtons}
    </Stack>
  );
};

export default PipelinesTable;
