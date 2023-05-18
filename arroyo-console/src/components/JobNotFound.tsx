import React from 'react';
import { Link, Text, VStack } from '@chakra-ui/react';

export interface JobErrorProps {
  icon: React.ReactElement;
  message: string;
}

const JobNotFound: React.FC<JobErrorProps> = ({ icon, message }) => {
  return (
    <VStack justify={'center'} height={'90vh'} spacing={30}>
      {icon}
      <VStack>
        <Text fontSize="xl">{message}</Text>
        <Text>
          <Link color="blue.400" href={'/jobs'}>
            Back to all jobs
          </Link>
        </Text>
      </VStack>
    </VStack>
  );
};

export default JobNotFound;
