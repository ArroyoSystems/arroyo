import { Spinner, VStack } from '@chakra-ui/react';
import React from 'react';

export interface LoadingProps {
  size?: string;
}

const Loading: React.FC<LoadingProps> = ({ size = 'xl' }) => {
  return (
    <VStack justify={'center'} height={'75%'} spacing={30}>
      <Spinner speed="0.65s" emptyColor="gray.200" color="blue.500" size={size} />
    </VStack>
  );
};

export default Loading;
