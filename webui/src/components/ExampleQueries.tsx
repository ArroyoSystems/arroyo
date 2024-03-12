import React, { useContext } from 'react';
import {
  Drawer,
  DrawerBody,
  DrawerCloseButton,
  DrawerContent,
  DrawerHeader,
  Flex,
  Popover,
  PopoverArrow,
  PopoverBody,
  PopoverCloseButton,
  PopoverContent,
  PopoverHeader,
  PopoverTrigger,
  Text,
} from '@chakra-ui/react';
import ExampleQueryCard from './ExampleQueryCard';
import { exampleQueries } from '../lib/example_queries';
import { TourContext, TourSteps } from '../tour';

export interface ExampleQueriesProps {
  isOpen: boolean;
  onClose: () => void;
  setQuery: (s: string) => void;
}

const ExampleQueries: React.FC<ExampleQueriesProps> = ({ isOpen, onClose, setQuery }) => {
  const { tourStep, disableTour } = useContext(TourContext);

  return (
    <Drawer isOpen={isOpen} placement="right" onClose={onClose} size={'xl'}>
      <DrawerContent padding={4} bg={'gray.600'}>
        <DrawerCloseButton onClick={onClose} />
        <DrawerHeader bg={'gray.600'}>
          Example Queries
          <Text fontSize={'md'}>
            Below are some example queries to show you the basics of building a pipeline with
            Arroyo.
          </Text>
        </DrawerHeader>
        <Popover
          isOpen={tourStep == TourSteps.ExampleQuery}
          placement={'left'}
          closeOnBlur={false}
          variant={'tour'}
          gutter={20}
        >
          <PopoverTrigger>
            <DrawerBody bg={'gray.600'}>
              <Flex flexDirection={'column'} gap={3}>
                {exampleQueries.map(example => (
                  <ExampleQueryCard example={example} setQuery={setQuery} key={example.name} />
                ))}
              </Flex>
            </DrawerBody>
          </PopoverTrigger>
          <PopoverContent>
            <PopoverArrow />
            <PopoverCloseButton onClick={disableTour} />
            <PopoverHeader>Example Queries</PopoverHeader>
            <PopoverBody>Explore the example queries and copy one to the editor.</PopoverBody>
          </PopoverContent>
        </Popover>
      </DrawerContent>
    </Drawer>
  );
};

export default ExampleQueries;
