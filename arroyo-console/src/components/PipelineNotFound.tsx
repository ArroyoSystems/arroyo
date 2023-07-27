import React from 'react';
import { Link, Text, VStack } from '@chakra-ui/react';

export interface PipelineNotFoundProps {
  icon: React.ReactElement;
  message: string;
}

const PipelineNotFound: React.FC<PipelineNotFoundProps> = ({ icon, message }) => {
  return (
    <VStack justify={'center'} height={'90vh'} spacing={30}>
      {icon}
      <VStack>
        <Text fontSize="xl">{message}</Text>
        <Text>
          <Link color="blue.400" href={'/jobs'}>
            Back to all pipelines
          </Link>
        </Text>
      </VStack>
    </VStack>
  );
};

export default PipelineNotFound;
