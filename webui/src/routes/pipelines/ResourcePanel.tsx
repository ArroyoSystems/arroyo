import React from 'react';
import { Flex, HStack, Stack, Tab, TabList, TabPanel, TabPanels, Tabs } from '@chakra-ui/react';
import CatalogTab from './CatalogTab';
import UdfsResourceTab from '../udfs/UdfsResourceTab';

export interface ResourcePanelProps {
  tabIndex: number;
  handleTabChange: (index: number) => void;
}

const ResourcePanel: React.FC<ResourcePanelProps> = ({ tabIndex, handleTabChange }) => {
  return (
    <Stack
      width={300}
      background="gray.900"
      spacing={2}
      borderRight={'1px solid'}
      borderColor={'gray.500'}
      overflow={'auto'}
    >
      <Tabs
        display={'flex'}
        flexDirection={'column'}
        flex={1}
        index={tabIndex}
        onChange={handleTabChange}
      >
        <TabList>
          <Flex justifyContent={'space-between'} width={'100%'}>
            <Flex>
              <Tab>Sources/Sinks</Tab>
              <Tab>UDFs</Tab>
            </Flex>
            <HStack py={9}>{}</HStack>
          </Flex>
        </TabList>
        <TabPanels flex={1}>
          <TabPanel height={'100%'} display={'flex'} px={5}>
            <CatalogTab />
          </TabPanel>
          <TabPanel height={'100%'} px={5}>
            <UdfsResourceTab />
          </TabPanel>
        </TabPanels>
      </Tabs>
    </Stack>
  );
};

export default ResourcePanel;
