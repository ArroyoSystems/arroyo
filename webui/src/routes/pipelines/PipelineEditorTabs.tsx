import React, { Dispatch, useContext } from 'react';
import {
  Box,
  Button,
  Checkbox,
  Flex,
  FormControl,
  FormHelperText,
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
  Stack,
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
import { FiCheckCircle, FiPlay } from 'react-icons/fi';
import { IoRocketOutline } from 'react-icons/io5';
import { PreviewOptions } from './CreatePipeline';
import { Job } from '../../lib/data_fetching';

export interface PipelineEditorTabsProps {
  queryInput: string;
  previewing: boolean | undefined;
  startingPreview: boolean;
  preview: () => void;
  stopPreview: () => void;
  run: () => void;
  pipelineIsValid: (tab: number) => void;
  updateQuery: (s: string) => void;
  previewOptions: PreviewOptions;
  setPreviewOptions: Dispatch<PreviewOptions>;
  job?: Job;
}

const PipelineEditorTabs: React.FC<PipelineEditorTabsProps> = ({
  queryInput,
  previewing,
  startingPreview,
  preview,
  stopPreview,
  run,
  pipelineIsValid,
  updateQuery,
  previewOptions,
  setPreviewOptions,
  job,
}) => {
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
          title="Example queries"
          size={'xs'}
          mr={4}
          borderColor={'gray.500'}
          borderWidth={'1px'}
          px={2}
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
    <Flex
      flexWrap={'nowrap'}
      overflowY={'hidden'}
      overflowX={'auto'}
      style={{ scrollbarWidth: 'none' }}
    >
      <Tab gap={1} height={10}>
        <Icon as={PiFileSqlDuotone} boxSize={4} />
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

  let startPreviewButton = <></>;
  let stopPreviewButton = <></>;

  if (previewing) {
    stopPreviewButton = (
      <Button
        onClick={stopPreview}
        size="xs"
        colorScheme="blue"
        title="Stop a preview pipeline"
        borderRadius={2}
        disabled={job?.state != 'Running'}
      >
        Stop
      </Button>
    );
  } else {
    startPreviewButton = (
      <Button
        onClick={preview}
        size="xs"
        title="Run a preview pipeline"
        borderRadius={2}
        isLoading={startingPreview}
        backgroundColor={'gray.700'}
        borderColor={'gray.500'}
        borderWidth={'1px'}
      >
        <HStack spacing={2}>
          <Icon as={FiPlay}></Icon>
          <Text>Preview</Text>
        </HStack>
      </Button>
    );
  }

  if (tourStep == TourSteps.Preview) {
    startPreviewButton = (
      <Popover isOpen={true} placement={'top'} closeOnBlur={false} variant={'tour'}>
        <PopoverTrigger>{startPreviewButton}</PopoverTrigger>
        <PopoverContent>
          <PopoverArrow />
          <PopoverCloseButton onClick={disableTour} />
          <PopoverHeader>Nice!</PopoverHeader>
          <PopoverBody>
            Finally, run a preview pipeline to see the results of your query.
          </PopoverBody>
        </PopoverContent>
      </Popover>
    );
  } else if (!previewing) {
    startPreviewButton = (
      <Popover trigger="hover">
        <PopoverTrigger>{startPreviewButton}</PopoverTrigger>
        <PopoverContent mt={2}>
          <PopoverArrow />
          <PopoverHeader>
            <Text fontWeight={'bold'} p={2} my={0} fontSize={'sm'}>
              Preview options
            </Text>
          </PopoverHeader>
          <PopoverBody p={4}>
            <Stack>
              <FormControl>
                <Checkbox
                  checked={previewOptions.enableSinks}
                  onChange={e =>
                    setPreviewOptions({ ...previewOptions, enableSinks: e.target.checked })
                  }
                >
                  Enable sinks
                </Checkbox>
                <FormHelperText fontSize={'xs'}>
                  By default, sinks are disabled in preview mode
                </FormHelperText>
              </FormControl>
            </Stack>
          </PopoverBody>
        </PopoverContent>
      </Popover>
    );
  }

  const checkButton = (
    <Button
      size="xs"
      color={'white'}
      backgroundColor={'gray.700'}
      onClick={() => pipelineIsValid(0)}
      title="Check that the SQL is valid"
      borderRadius={2}
      borderColor={'gray.500'}
      borderWidth={'1px'}
    >
      <HStack spacing={2}>
        <Icon as={FiCheckCircle}></Icon>
        <Text>Check</Text>
      </HStack>
    </Button>
  );

  const startPipelineButton = (
    <Button
      size="xs"
      backgroundColor={'gray.700'}
      onClick={run}
      borderRadius={2}
      color="green.200"
      borderColor={'gray.500'}
      borderWidth={'1px'}
    >
      <HStack>
        <Icon as={IoRocketOutline} />
        <Text>Launch</Text>
      </HStack>
    </Button>
  );

  return (
    <Flex direction={'column'} backgroundColor="#1e1e1e" height="100%">
      <Tabs
        size={'sm'}
        display={'flex'}
        flexDirection={'column'}
        flex={1}
        index={editorTab}
        onChange={handleEditorTabChange}
      >
        <HStack spacing={0}>
          <TabList
            display={'grid'}
            gridTemplateColumns={'minmax(0, 1fr) min-content'}
            width={'100%'}
            h={10}
          >
            {tabs}
          </TabList>
          <HStack
            backgroundColor={'gray.700'}
            border={'1px solid #111'}
            boxShadow={'-5px 5px 15px #00000044'}
            h="100%"
            pl={4}
            spacing={2}
          >
            {checkButton}
            {startPreviewButton}
            {stopPreviewButton}
            {startPipelineButton}
            {exampleQueriesButton}
          </HStack>
        </HStack>
        {tabPanels}
      </Tabs>
      {exampleQueries}
    </Flex>
  );
};

export default PipelineEditorTabs;
