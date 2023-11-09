import React, { useState } from 'react';
import {
  Accordion,
  AccordionButton,
  AccordionIcon,
  AccordionItem,
  AccordionPanel,
  Box,
  Flex,
  Icon,
  Text,
} from '@chakra-ui/react';
import { UdfNode } from '../routes/udfs/UdfsResourceTab';
import { AiOutlineFolder } from 'react-icons/ai';
import UdfNodePopover from '../routes/udfs/UdfNodePopover';

export interface UdfNodeViewerProps {
  node: UdfNode;
}

const UdfNodeViewer: React.FC<UdfNodeViewerProps> = ({ node }) => {
  const [index, setIndex] = useState(0);

  if (!('folderName' in node)) {
    return <UdfNodePopover udf={node} />;
  }

  return (
    <Box pl={1}>
      <Accordion
        allowToggle
        key={node.folderName}
        index={index}
        onChange={(i: number) => setIndex(i)}
      >
        <AccordionItem key={node.folderName} border={'none'}>
          <AccordionButton padding={0}>
            <Flex width={'100%'} justifyContent={'space-between'} alignItems={'center'}>
              <Flex alignItems={'center'} gap={1}>
                <Icon as={AiOutlineFolder} />
                <Text fontSize={'md'}>{node.folderName}</Text>
              </Flex>
              <AccordionIcon />
            </Flex>
          </AccordionButton>
          <AccordionPanel padding={0} pl={5}>
            {node.children.map(child => (
              <UdfNodeViewer node={child} key={child.id} />
            ))}
          </AccordionPanel>
        </AccordionItem>
      </Accordion>
    </Box>
  );
};

export default UdfNodeViewer;
