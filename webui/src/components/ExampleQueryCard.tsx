import React, { useContext } from 'react';
import {
  Accordion,
  AccordionButton,
  AccordionIcon,
  AccordionItem,
  AccordionPanel,
  Box,
  Button,
  Card,
  Flex,
  Link,
  Text,
} from '@chakra-ui/react';
import SyntaxHighlighter from 'react-syntax-highlighter';
import { ExampleQuery } from '../lib/example_queries';
import { ExternalLinkIcon } from '@chakra-ui/icons';
import { vs2015 } from 'react-syntax-highlighter/dist/esm/styles/hljs';
import { TourContext, TourSteps } from '../tour';

export interface ExampleQueryCardProps {
  example: ExampleQuery;
  setQuery: (s: string) => void;
}

const ExampleQueryCard: React.FC<ExampleQueryCardProps> = ({ example, setQuery }) => {
  const { setTourStep } = useContext(TourContext);

  const buttonPart = (
    <Flex alignItems={'stretch'}>
      <AccordionButton>
        <Flex flex="1" textAlign="left" justifyContent={'space-between'}>
          <Flex alignItems={'center'} gap={4}>
            <AccordionIcon />
            <div>
              <Text fontSize="xl">{example.name}</Text>
              <Text fontSize="md">{example.shortDescription}</Text>
            </div>
          </Flex>
        </Flex>
      </AccordionButton>
      <div>
        <Button
          height={'100%'}
          borderRadius={'0 6px 6px 0'}
          onClick={() => {
            setQuery(example.query);
            setTourStep(TourSteps.Preview);
          }}
        >
          Copy to Editor
        </Button>
      </div>
    </Flex>
  );

  let link = <></>;
  if (example.url) {
    link = (
      <Text align={'right'} fontSize={'sm'}>
        <Link href={example.url} isExternal>
          See the full tutorial here
          <ExternalLinkIcon mx="2px" />
        </Link>
      </Text>
    );
  }

  const contentPart = (
    <AccordionPanel borderTop={'1px solid #1A202C'}>
      <Text fontSize={'sm'} paddingTop={2}>
        {example.longDescription}
      </Text>
      {link}
      <Box borderRadius={10} paddingTop={3}>
        <SyntaxHighlighter language="sql" style={vs2015} customStyle={{ borderRadius: '5px' }}>
          {example.query}
        </SyntaxHighlighter>
      </Box>
    </AccordionPanel>
  );

  return (
    <Card>
      <Accordion allowToggle>
        <AccordionItem border={'none'}>
          {buttonPart}
          {contentPart}
        </AccordionItem>
      </Accordion>
    </Card>
  );
};

export default ExampleQueryCard;
