import { useNavigate } from 'react-router-dom';
import {
  Alert,
  AlertDescription,
  AlertIcon,
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
import { JobStatus } from '../gen/api_pb';
import { usePipelines } from '../lib/data_fetching';
import PipelineRow from './PipelineRow';

export interface JobsTableProps {}

const columns = ['name', 'job'];

const PipelinesTable: React.FC<JobsTableProps> = ({}) => {
  const navigate = useNavigate();
  // const [state, setState] = useState<Array<JobStatus> | null>();

  const { isOpen, onOpen, onClose } = useDisclosure();
  const cancelRef = useRef<HTMLButtonElement>(null);
  const [message, setMessage] = useState<{ text: string; type: 'success' | 'error' } | null>(null);
  const [jobToBeDeleted, setJobToBeDelete] = useState<JobStatus | null>(null);
  const { pipelines } = usePipelines();

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

  // const deleteJob = async (job: JobStatus) => {
  //   onClose();
  //   try {
  //     await (
  //       await client()
  //     ).deleteJob(
  //       new DeleteJobReq({
  //         jobId: job.jobId,
  //       })
  //     );
  //     setMessage({ text: `Job ${job.jobId} successfully deleted`, type: 'success' });
  //   } catch (e) {
  //     if (e instanceof ConnectError) {
  //       setMessage({ text: e.rawMessage, type: 'error' });
  //     } else {
  //       setMessage({ text: 'Something went wrong', type: 'error' });
  //     }
  //   }
  // };

  // const alert = (
  //   <AlertDialog isOpen={isOpen} leastDestructiveRef={cancelRef} onClose={onClose}>
  //     <AlertDialogOverlay>
  //       <AlertDialogContent>
  //         <AlertDialogHeader fontSize="lg" fontWeight="bold">
  //           Delete Job {jobToBeDeleted?.pipelineName}
  //         </AlertDialogHeader>
  //
  //         <AlertDialogBody>
  //           Are you sure you want to delete job {jobToBeDeleted?.jobId}? Job state will be lost.
  //         </AlertDialogBody>
  //
  //         <AlertDialogFooter>
  //           <Button ref={cancelRef} onClick={onClose}>
  //             Cancel
  //           </Button>
  //           <Button colorScheme="red" onClick={() => deleteJob(jobToBeDeleted!)} ml={3}>
  //             Delete
  //           </Button>
  //         </AlertDialogFooter>
  //       </AlertDialogContent>
  //     </AlertDialogOverlay>
  //   </AlertDialog>
  // );

  const tableHeader = (
    <Thead>
      <Tr>
        <Th>Id</Th>
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
      {pipelines?.flatMap(pipeline => (
        <PipelineRow pipeline={pipeline} />
      ))}
    </Tbody>
  );

  return (
    <Stack spacing={2}>
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
