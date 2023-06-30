import {
  Alert,
  AlertDescription,
  AlertIcon,
  Button,
  FormControl,
  FormHelperText,
  FormLabel,
  Input,
  Modal,
  ModalBody,
  ModalCloseButton,
  ModalContent,
  ModalFooter,
  ModalHeader,
  ModalOverlay,
  Stack,
} from '@chakra-ui/react';
import React from 'react';
import { SqlOptions } from '../lib/types';

export interface StartPipelineModalProps {
  isOpen: boolean;
  onClose: () => void;
  startError: string | null;
  options: SqlOptions;
  setOptions: (s: SqlOptions) => void;
  start: () => void;
}

const StartPipelineModal: React.FC<StartPipelineModalProps> = ({
  isOpen,
  onClose,
  startError,
  options,
  setOptions,
  start,
}) => {
  return (
    <Modal isOpen={isOpen} onClose={onClose} isCentered>
      <ModalOverlay />
      <ModalContent>
        <ModalHeader>Start Pipeline</ModalHeader>
        <ModalCloseButton />
        <ModalBody>
          <Stack spacing={8}>
            {startError ? (
              <Alert status="error">
                <AlertIcon />
                <AlertDescription>{startError}</AlertDescription>
              </Alert>
            ) : null}

            <FormControl>
              <FormLabel>Name</FormLabel>
              <Input
                type="text"
                value={options.name || ''}
                onChange={v => setOptions({ ...options, name: v.target.value })}
              />
              <FormHelperText>Give this pipeline a name to help you identify it</FormHelperText>
            </FormControl>
          </Stack>
        </ModalBody>

        <ModalFooter>
          <Button mr={3} onClick={onClose}>
            Cancel
          </Button>
          <Button
            variant="primary"
            onClick={start}
            isDisabled={options.name == '' || options.parallelism == null}
          >
            Start
          </Button>
        </ModalFooter>
      </ModalContent>
    </Modal>
  );
};

export default StartPipelineModal;
