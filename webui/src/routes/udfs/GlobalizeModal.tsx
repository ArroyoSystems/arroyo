import {
  Alert,
  AlertDescription,
  AlertIcon,
  Button,
  Code,
  FormControl,
  FormHelperText,
  FormLabel,
  Input,
  InputGroup,
  InputLeftAddon,
  Modal,
  ModalBody,
  ModalCloseButton,
  ModalContent,
  ModalFooter,
  ModalHeader,
  ModalOverlay,
  Text,
  Textarea,
} from '@chakra-ui/react';
import { useGlobalUdfs } from '../../lib/data_fetching';
import React, { useContext, useState } from 'react';
import { formatError } from '../../lib/util';
import { LocalUdf, LocalUdfsContext } from '../../udf_state';

export interface GlobalizeModalProps {
  isOpen: boolean;
  onClose: () => void;
  udf: LocalUdf;
}

const GlobalizeModal: React.FC<GlobalizeModalProps> = ({ isOpen, onClose, udf }) => {
  const [prefix, setPrefix] = useState<string>('');
  const [description, setDescription] = useState<string>('');
  const { createGlobalUdf } = useGlobalUdfs();
  const [createError, setCreateError] = useState<string | undefined>(undefined);
  const { deleteLocalUdf } = useContext(LocalUdfsContext);

  let cleanPrefix = '/';
  if (prefix) {
    // remove duplicate slashes, ensure leading and trailing slash
    cleanPrefix = '/' + prefix.replace(/^\/*|\/*$/g, '').replace(/\/+/g, '/') + '/';
  }

  const share = async () => {
    const { error } = await createGlobalUdf(cleanPrefix, udf.definition, description);

    if (error) {
      setCreateError(formatError(error));
      return;
    }

    deleteLocalUdf(udf);
    onClose();
  };

  const handleClose = () => {
    onClose();
    setPrefix('');
    setDescription('');
    setCreateError(undefined);
  };

  if ('errors' in udf && udf.errors?.length) {
    return (
      <Modal isOpen={isOpen} onClose={handleClose} isCentered size={'lg'}>
        <ModalOverlay />
        <ModalContent>
          <ModalHeader>Globalize UDF</ModalHeader>
          <ModalCloseButton />
          <ModalBody>
            <Alert status="error">
              <AlertIcon />
              <AlertDescription>
                Validation error.
                <br />
                Please resolve the error and try again.
              </AlertDescription>
            </Alert>
          </ModalBody>
          <ModalFooter>
            <Button mr={3} onClick={handleClose}>
              Ok
            </Button>
          </ModalFooter>
        </ModalContent>
      </Modal>
    );
  }

  let errorAlert = <></>;
  if (createError) {
    errorAlert = (
      <Alert status="error">
        <AlertIcon />
        <AlertDescription>{createError}</AlertDescription>
      </Alert>
    );
  }

  return (
    <Modal isOpen={isOpen} onClose={handleClose} isCentered size={'lg'} z-index={6}>
      <ModalOverlay />
      <ModalContent
        // Stop key presses from applying to the edit tabs behind the modal
        onKeyDown={e => {
          if (e.key === 'Escape') {
            handleClose();
          }
          e.stopPropagation();
        }}
      >
        <ModalHeader>Globalize UDF</ModalHeader>
        <ModalCloseButton />
        <ModalBody display={'flex'} flexDirection={'column'} gap={3}>
          <Text>
            Change the scope of the UDF to use it across all pipelines.
            <br />
            Note: Global UDFs cannot be edited.
          </Text>
          <FormControl>
            <FormLabel>Description (optional)</FormLabel>
            <Textarea onChange={v => setDescription(v.target.value)} value={description} />
          </FormControl>
          <FormControl>
            <FormLabel>Folder Prefix (optional)</FormLabel>
            <InputGroup>
              <InputLeftAddon children={<Code>/</Code>} px={1} bg={'gray.800'} />
              <Input
                type="text"
                onChange={v => setPrefix(v.target.value)}
                value={prefix}
                size={'md'}
              />
            </InputGroup>
            <FormHelperText>Example: folder1/folder2/ </FormHelperText>
          </FormControl>
          <Text fontWeight={'bold'}>
            Full path:{' '}
            <Code>
              {cleanPrefix}
              {udf.name}
            </Code>
          </Text>
          {errorAlert}
        </ModalBody>
        <ModalFooter>
          <Button mr={3} onClick={handleClose}>
            Cancel
          </Button>
          <Button variant="primary" onClick={share}>
            Continue
          </Button>
        </ModalFooter>
      </ModalContent>
    </Modal>
  );
};

export default GlobalizeModal;
