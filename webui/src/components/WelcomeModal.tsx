import React, { useContext } from 'react';
import {
  Button,
  Link,
  Modal,
  ModalBody,
  ModalCloseButton,
  ModalContent,
  ModalFooter,
  ModalOverlay,
  Text,
} from '@chakra-ui/react';
import { TourContext, TourSteps } from '../tour';

const WelcomeModal: React.FC = () => {
  const { tourStep, setTourStep, disableTour } = useContext(TourContext);

  return (
    <Modal
      isOpen={tourStep == TourSteps.WelcomeModal}
      onClose={() => {}}
      isCentered
      size={'xl'}
      variant={'tour'}
    >
      <ModalOverlay />
      <ModalContent padding={8}>
        <ModalCloseButton onClick={disableTour} />
        <ModalBody display={'flex'} gap={6} flexDirection={'column'} alignItems={'center'}>
          <img width="180" src="/logo.svg" />
          <Text fontSize={'md'}>
            Welcome! Arroyo is a distributed stream processing engine, designed to efficiently
            perform stateful computations on streams of data.
            <br />
            <br />
            Want some help running your first pipeline?
          </Text>
        </ModalBody>
        <ModalFooter
          display={'flex'}
          flexDirection={'column'}
          gap={'5px'}
          justifyContent={'center'}
        >
          <Button colorScheme={'blue'} onClick={() => setTourStep(TourSteps.CreatePipelineButton)}>
            Let's do it!
          </Button>
          <Link>
            <Text fontSize={'sm'} color={'grey.200'} onClick={disableTour}>
              No thanks
            </Text>
          </Link>
        </ModalFooter>
      </ModalContent>
    </Modal>
  );
};

export default WelcomeModal;
