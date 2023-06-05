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
  Select,
  Stack,
} from '@chakra-ui/react';
import React from 'react';
import { SinkOpt, SqlOptions } from '../lib/types';

export interface StartPipelineModalProps {
  isOpen: boolean;
  onClose: () => void;
  startError: string | null;
  options: SqlOptions;
  setOptions: (s: SqlOptions) => void;
  sinks: Array<SinkOpt>;
  start: () => void;
}

const StartPipelineModal: React.FC<StartPipelineModalProps> = ({
  isOpen,
  onClose,
  startError,
  options,
  setOptions,
  sinks,
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
            <FormControl>
              <FormLabel>Sink</FormLabel>
              <Select
                variant="filled"
                value={options.sink}
                onChange={v =>
                  setOptions({
                    ...options,
                    sink: v.target.value ? Number(v.target.value) : undefined,
                  })
                }
                placeholder="Select sink"
              >
                {sinks.map((s, i) => (
                  <option key={s.name} value={i}>
                    {s.name}
                  </option>
                ))}
              </Select>
              <FormHelperText>Choose where the outputs of the pipeline will go</FormHelperText>
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
            isDisabled={options.name == '' || options.parallelism == null || options.sink == null}
          >
            Start
          </Button>
        </ModalFooter>
      </ModalContent>
    </Modal>
  );
};

export default StartPipelineModal;
