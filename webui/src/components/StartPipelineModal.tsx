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
  NumberInputField,
  NumberInput,
  Stack,
  NumberInputStepper,
  NumberIncrementStepper,
  NumberDecrementStepper,
  Box,
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
    <Modal isOpen={isOpen} onClose={onClose} isCentered size={startError ? '4xl' : 'xl'}>
      <ModalOverlay />
      <ModalContent>
        <ModalHeader>Start Pipeline</ModalHeader>
        <ModalCloseButton />
        <ModalBody>
          <Stack spacing={8}>
            {startError ? (
              <Alert status="error">
                <AlertIcon />
                <AlertDescription overflowY={'auto'} maxH={400} whiteSpace={'pre-wrap'}>
                  {startError}
                </AlertDescription>
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

            <FormControl>
              <FormLabel>Parallelism</FormLabel>
              <Box>
                <NumberInput
                  step={1}
                  min={1}
                  max={1024}
                  bg={'gray.800'}
                  value={options.parallelism || 1}
                  onChange={v => setOptions({ ...options, parallelism: Number(v) })}
                >
                  <NumberInputField />
                  <NumberInputStepper>
                    <NumberIncrementStepper />
                    <NumberDecrementStepper />
                  </NumberInputStepper>
                </NumberInput>
              </Box>
              <FormHelperText>
                How many parallel subtasks should be used for this pipeline
              </FormHelperText>
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
