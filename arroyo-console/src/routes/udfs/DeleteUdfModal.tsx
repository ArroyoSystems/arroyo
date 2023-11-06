import React, { useContext } from 'react';
import {
  AlertDialog,
  AlertDialogBody,
  AlertDialogContent,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogOverlay,
  Text,
  Button,
} from '@chakra-ui/react';
import { LocalUdf, LocalUdfsContext } from '../../udf_state';
import { GlobalUdf } from '../../lib/data_fetching';

export interface DeleteUdfModalProps {
  udf: LocalUdf | GlobalUdf;
  deleteModalIsOpen: boolean;
  deleteModalOnClose: () => void;
  cancelDeleteRef: React.RefObject<HTMLButtonElement>;
}

const DeleteUdfModal: React.FC<DeleteUdfModalProps> = ({
  udf,
  deleteModalOnClose,
  deleteModalIsOpen,
  cancelDeleteRef,
}) => {
  const { deleteUdf, isGlobal } = useContext(LocalUdfsContext);

  let title = 'Delete Local UDF';
  if (isGlobal(udf)) {
    title = 'Delete Global UDF';
  }

  return (
    <AlertDialog
      leastDestructiveRef={cancelDeleteRef}
      isOpen={deleteModalIsOpen}
      onClose={deleteModalOnClose}
    >
      <AlertDialogOverlay>
        <AlertDialogContent>
          <AlertDialogHeader fontSize="lg" fontWeight="bold">
            {title}
          </AlertDialogHeader>
          <AlertDialogBody display={'flex'} flexDirection={'column'} gap={3}>
            <Text>
              Are you sure you want to delete the UDF?
              <br />
              This action cannot be undone.
            </Text>
          </AlertDialogBody>
          <AlertDialogFooter>
            <Button ref={cancelDeleteRef} onClick={deleteModalOnClose}>
              Cancel
            </Button>
            <Button
              colorScheme="red"
              onClick={() => {
                deleteUdf(udf);
                deleteModalOnClose();
              }}
              ml={3}
            >
              Delete
            </Button>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialogOverlay>
    </AlertDialog>
  );
};

export default DeleteUdfModal;
