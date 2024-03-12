import React, { useContext } from 'react';
import {
  Box,
  Flex,
  HStack,
  Icon,
  IconButton,
  Popover,
  PopoverArrow,
  PopoverBody,
  PopoverCloseButton,
  PopoverContent,
  PopoverHeader,
  PopoverTrigger,
  Tab,
  TabList,
  TabPanel,
  TabPanels,
  Tabs,
  Text,
  useDisclosure,
} from '@chakra-ui/react';
import { CodeEditor } from './CodeEditor';
import { TourContext, TourSteps } from '../../tour';
import { HiOutlineBookOpen } from 'react-icons/hi';
import ExampleQueries from '../../components/ExampleQueries';
import { PiFileSqlDuotone } from 'react-icons/pi';
import { LocalUdfsContext } from '../../udf_state';
import UdfEditor from '../udfs/UdfEditor';
import UdfEditTab from '../udfs/UdfEditTab';

export interface PipelineEditorTabsProps {
  queryInput: string;
  updateQuery: (s: string) => void;
}

const PipelineEditorTabs: React.FC<PipelineEditorTabsProps> = ({ queryInput, updateQuery }) => {
  const { openedUdfs, isGlobal, editorTab, handleEditorTabChange } = useContext(LocalUdfsContext);
  const {
    isOpen: exampleQueriesIsOpen,
    onOpen: openExampleQueries,
    onClose: onExampleQueriesClose,
  } = useDisclosure();
  const { tourStep, setTourStep, disableTour } = useContext(TourContext);

  const exampleQueries = (
    <ExampleQueries
      isOpen={exampleQueriesIsOpen}
      onClose={onExampleQueriesClose}
      setQuery={s => {
        updateQuery(s);
        onExampleQueriesClose();
      }}
    />
  );

  const exampleQueriesButton = (
    <Popover
      isOpen={tourStep == TourSteps.ExampleQueriesButton}
      placement={'bottom-start'}
      closeOnBlur={false}
      variant={'tour'}
    >
      <PopoverTrigger>
        <IconButton
          icon={<HiOutlineBookOpen />}
          aria-label="Example queries"
          onClick={() => {
            openExampleQueries();
            setTourStep(TourSteps.ExampleQuery);
          }}
        />
      </PopoverTrigger>
      <PopoverContent>
        <PopoverArrow />
        <PopoverCloseButton onClick={disableTour} />
        <PopoverHeader>Example Queries</PopoverHeader>
        <PopoverBody>Open for some example queries</PopoverBody>
      </PopoverContent>
    </Popover>
  );

  const tabs = (
    <Flex flexWrap={'wrap'}>
      <Tab gap={1}>
        <Icon as={PiFileSqlDuotone} boxSize={5} />
        Query
      </Tab>
      {openedUdfs.map(udf => {
        return <UdfEditTab key={udf.id} udf={udf} />;
      })}
    </Flex>
  );

  const tabPanels = (
    <TabPanels flex={1}>
      <TabPanel height={'100%'} p={0} display={'flex'}>
        <CodeEditor code={queryInput} setCode={updateQuery} />
      </TabPanel>
      {openedUdfs.map(udf => {
        let globalBanner = <></>;
        if (isGlobal(udf)) {
          globalBanner = (
            <Box backgroundColor="blue.700" px={3} py={1}>
              <Text fontStyle={'italic'} align={'center'}>
                Global UDFs cannot be edited
              </Text>
            </Box>
          );
        }

        return (
          <TabPanel
            height={'100%'}
            key={udf.id}
            display={'flex'}
            flexDirection={'column'}
            gap={3}
            p={0}
          >
            {globalBanner}
            <UdfEditor udf={udf} />
          </TabPanel>
        );
      })}
    </TabPanels>
  );

  return (
    <Flex direction={'column'} backgroundColor="#1e1e1e" height="100%">
      <Tabs
        display={'flex'}
        flexDirection={'column'}
        flex={1}
        index={editorTab}
        onChange={handleEditorTabChange}
      >
        <TabList display={'grid'} gridTemplateColumns={'minmax(0, 1fr) min-content'} width={'100%'}>
          {tabs}
          <HStack p={4}>{exampleQueriesButton}</HStack>
        </TabList>
        {tabPanels}
      </Tabs>
      {exampleQueries}
    </Flex>
  );
};

export default PipelineEditorTabs;
