import {
  AlertDialog,
  AlertDialogBody,
  AlertDialogContent,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogOverlay,
  Button,
} from '@chakra-ui/react';
import React, { RefObject } from 'react';
import { JobStatus } from '../gen/api_pb';

export interface DeleteJobModalProps {
  isOpen: boolean;
  cancelRef: RefObject<HTMLButtonElement>;
  onClose: () => void;
  jobToBeDeleted: JobStatus | null;
  deleteJob: (jobStatus: JobStatus) => void;
}

const DeleteJobModal: React.FC<DeleteJobModalProps> = ({
  isOpen,
  cancelRef,
  onClose,
  jobToBeDeleted,
  deleteJob,
}) => {
  return (
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
  );
};

export default DeleteJobModal;
