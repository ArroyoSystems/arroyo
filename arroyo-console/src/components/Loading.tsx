import { Flex, Spinner } from '@chakra-ui/react';
import React from 'react';

export interface LoadingProps {
  size?: string;
}

const Loading: React.FC<LoadingProps> = ({ size = 'xl' }) => {
  return (
    <Flex border={'blue.900'} borderWidth={'1px'} justify={'center'} height={'75%'} p={8}>
      <Spinner speed="0.65s" emptyColor="gray.200" color="blue.900" size={size} />
    </Flex>
  );
};

export default Loading;
