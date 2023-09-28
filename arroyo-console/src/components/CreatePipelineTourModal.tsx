import React, { useContext } from 'react';
import {
  Button,
  Icon,
  Modal,
  ModalBody,
  ModalCloseButton,
  ModalContent,
  ModalFooter,
  ModalOverlay,
  Text,
} from '@chakra-ui/react';
import { TourContext, TourSteps } from '../tour';
import { FiGitBranch } from 'react-icons/fi';

const CreatePipelineTourModal: React.FC = () => {
  const { tourStep, setTourStep, disableTour } = useContext(TourContext);

  return (
    <Modal
      isOpen={tourStep == TourSteps.CreatePipelineModal}
      onClose={() => {}}
      isCentered
      variant={'tour'}
    >
      <ModalOverlay />
      <ModalContent padding={3}>
        <ModalCloseButton color={'back'} onClick={disableTour} />
        <ModalBody display={'flex'} gap={3} flexDirection={'column'} alignItems={'center'}>
          <Icon as={FiGitBranch} boxSize={55} />
          <Text fontSize={'lg'} fontWeight={'bold'} textAlign={'center'}>
            Pipeline Builder
          </Text>
          <Text fontSize={'md'} textAlign={'center'}>
            On this page you can write your SQL queries and UDFs, see a graph of your pipeline, and
            preview the output. Let's check out an example query...
          </Text>
        </ModalBody>
        <ModalFooter>
          <Button
            colorScheme={'blue'}
            onClick={() => {
              setTourStep(TourSteps.ExampleQueriesButton);
            }}
          >
            Next
          </Button>
        </ModalFooter>
      </ModalContent>
    </Modal>
  );
};

export default CreatePipelineTourModal;
