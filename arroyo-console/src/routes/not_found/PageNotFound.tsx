import React from 'react';
import { Link, VStack, Text, Icon } from '@chakra-ui/react';
import { CiFaceFrown } from 'react-icons/all';

const PageNotFound: React.FC = ({}) => {
  return (
    <VStack justify={'center'} height={'90vh'} spacing={30}>
      <Icon as={CiFaceFrown} boxSize={55} />
      <VStack>
        <Text fontSize="xl">Sorry, page not found</Text>
        <Text>
          <Link color="blue.400" href={'/'}>
            Go back home
          </Link>
        </Text>
      </VStack>
    </VStack>
  );
};

export default PageNotFound;
