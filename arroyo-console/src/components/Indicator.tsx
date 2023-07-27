import React from 'react';
import { Flex, Text } from '@chakra-ui/react';

export interface IndicatorProps {
  content: string;
  label?: string;
}

const Indicator: React.FC<IndicatorProps> = ({ content, label }) => {
  return (
    <Flex direction={'column'} justifyContent={'center'} flex={'0 0 100px'}>
      <Text
        as="b"
        fontSize="lg"
        textOverflow={'ellipsis'}
        whiteSpace={'normal'}
        wordBreak={'break-all'}
        noOfLines={1}
      >
        {content}
      </Text>
      {label && <Text fontSize="xs">{label}</Text>}
    </Flex>
  );
};

export default Indicator;
