import React, { useState } from 'react';
import {
  Alert,
  AlertDescription,
  AlertIcon,
  Button,
  FormControl,
  FormErrorMessage,
  FormHelperText,
  FormLabel,
  Modal,
  ModalBody,
  ModalCloseButton,
  ModalContent,
  ModalFooter,
  ModalHeader,
  ModalOverlay,
  NumberDecrementStepper,
  NumberIncrementStepper,
  NumberInput,
  NumberInputField,
  NumberInputStepper,
} from '@chakra-ui/react';

export interface PipelineConfigModalProps {
  isOpen: boolean;
  parallelism: number;
  onClose: () => void;
  updateJobParallelism: (parallelism: number) => void;
}

const PipelineConfigModal: React.FC<PipelineConfigModalProps> = ({
  isOpen,
  parallelism,
  onClose,
  updateJobParallelism,
}) => {
  const [parallelismInputValue, setParallelismInputValue] = useState<number | undefined>(
    parallelism
  );

  const close = () => {
    onClose();

    // reset state
    setParallelismInputValue(parallelism);
  };

  const onConfirm = () => {
    if (parallelismInputValue) {
      updateJobParallelism(parallelismInputValue);
    }
    onClose();
  };

  const pipelineRequiresRestart = parallelismInputValue != parallelism;

  let changeAlert = <></>;
  if (pipelineRequiresRestart) {
    changeAlert = (
      <Alert status="warning">
        <AlertIcon />
        <AlertDescription>These changes require a pipeline restart.</AlertDescription>
      </Alert>
    );
  }

  const parallelismInput = (
    <NumberInput
      isRequired
      min={1}
      value={parallelismInputValue ?? ''}
      onChange={(valueAsString, valueAsNumber) => {
        if (isNaN(valueAsNumber)) {
          setParallelismInputValue(undefined);
        } else {
          setParallelismInputValue(valueAsNumber);
        }
      }}
    >
      <NumberInputField bg="gray.800" />
      <NumberInputStepper>
        <NumberIncrementStepper />
        <NumberDecrementStepper />
      </NumberInputStepper>
    </NumberInput>
  );

  let parallelismHasError = false;
  let parallelismErrorText = '';
  let saveEnabled: boolean;

  if (!parallelismInputValue) {
    parallelismHasError = true;
    parallelismErrorText = 'Enter a valid number.';
  }

  saveEnabled = !parallelismHasError;

  const parallelismForm = (
    <FormControl isRequired isInvalid={parallelismHasError}>
      <FormLabel>Parallelism</FormLabel>
      {parallelismInput}
      <FormHelperText>Number of parallel substacks for each node</FormHelperText>
      <FormErrorMessage>{parallelismErrorText}</FormErrorMessage>
    </FormControl>
  );

  return (
    <Modal isOpen={isOpen} onClose={close}>
      <ModalOverlay />
      <ModalContent>
        <ModalHeader>Edit Pipeline Configuration</ModalHeader>
        <ModalCloseButton />
        {changeAlert}
        <ModalBody>{parallelismForm}</ModalBody>
        <ModalFooter>
          <Button variant="ghost" onClick={onClose}>
            Cancel
          </Button>
          <Button colorScheme="blue" mr={3} isDisabled={!saveEnabled} onClick={onConfirm}>
            Save
          </Button>
        </ModalFooter>
      </ModalContent>
    </Modal>
  );
};

export default PipelineConfigModal;
