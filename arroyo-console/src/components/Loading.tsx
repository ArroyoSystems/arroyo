import { Flex, Spinner } from '@chakra-ui/react';
import React from 'react';

export interface LoadingProps {
  size?: string;
}

const Loading: React.FC<LoadingProps> = ({ size = 'xl' }) => {
  return (
    <Flex border={'1px solid blue'} justify={'center'} height={'75%'}>
      <Spinner speed="0.65s" emptyColor="gray.200" color="blue.500" size={size} />
    </Flex>
  );
};

export default Loading;
