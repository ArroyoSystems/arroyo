import React, { useContext } from 'react';
import { TourContext, TourSteps } from '../tour';
import {
  Button,
  Icon,
  Link,
  Modal,
  ModalBody,
  ModalCloseButton,
  ModalContent,
  ModalOverlay,
  Text,
} from '@chakra-ui/react';
import { LuPartyPopper } from 'react-icons/lu';

const TourCompleteModal: React.FC = () => {
  const { tourStep, disableTour } = useContext(TourContext);

  return (
    <Modal
      isOpen={tourStep == TourSteps.TourCompleted}
      onClose={() => {}}
      isCentered
      variant={'tour'}
      size={'lg'}
    >
      <ModalOverlay />
      <ModalContent padding={3}>
        <ModalCloseButton color={'back'} onClick={disableTour} />
        <ModalBody display={'flex'} gap={3} flexDirection={'column'} alignItems={'center'}>
          <Icon as={LuPartyPopper} boxSize={55} />
          <Text fontSize={'lg'} fontWeight={'bold'} textAlign={'center'}>
            Congratulations!
          </Text>
          <Text fontSize={'md'} textAlign={'center'}>
            You've officially run your first pipeline.
            <br />
            <br />
            Check out the{' '}
            <Link href={'https://doc.arroyo.dev/'} isExternal>
              tutorials
            </Link>{' '}
            and{' '}
            <Link href={'https://doc.arroyo.dev/'} isExternal>
              SQL reference
            </Link>{' '}
            for more information on how to get the most out of Arroyo.
          </Text>
          <Button colorScheme={'blue'} onClick={disableTour}>
            Done
          </Button>
        </ModalBody>
      </ModalContent>
    </Modal>
  );
};

export default TourCompleteModal;
