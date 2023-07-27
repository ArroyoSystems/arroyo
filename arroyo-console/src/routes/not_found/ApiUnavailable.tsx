import React from 'react';
import { Link, VStack, Text, Icon } from '@chakra-ui/react';
import { BiTransferAlt } from 'react-icons/bi';

const ApiUnavailable: React.FC = () => {
  return (
    <VStack justify={'center'} height={'90vh'} spacing={30}>
      <Icon as={BiTransferAlt} boxSize={55} />
      <VStack>
        <Text fontSize="xl">The API is currently unavailable.</Text>
        <Text>
          <Link color="blue.400" href={'/'}>
            Try again
          </Link>
        </Text>
      </VStack>
    </VStack>
  );
};

export default ApiUnavailable;
