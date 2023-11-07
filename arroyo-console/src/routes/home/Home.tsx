import {
  Box,
  Button,
  Container,
  Heading,
  HStack,
  Popover,
  PopoverArrow,
  PopoverBody,
  PopoverCloseButton,
  PopoverContent,
  PopoverHeader,
  PopoverTrigger,
  SimpleGrid,
  Stack,
  Text,
  useBreakpointValue,
  useColorModeValue,
} from '@chakra-ui/react';
import { useNavigate } from 'react-router-dom';
import { useJobs } from '../../lib/data_fetching';
import Loading from '../../components/Loading';
import React, { useContext, useEffect } from 'react';
import WelcomeModal from '../../components/WelcomeModal';
import { TourContext, TourSteps } from '../../tour';

interface Props {
  label: string;
  value?: string;
  color?: string;
}
export const Stat = (props: Props) => {
  const { label, value, color, ...boxProps } = props;
  return (
    <Box
      px={{ base: '4', md: '6' }}
      py={{ base: '5', md: '6' }}
      bg="bg-surface"
      height={120}
      borderRadius="lg"
      boxShadow={useColorModeValue('sm', 'sm-dark')}
      {...boxProps}
    >
      <Stack>
        <Text fontSize="sm" color="muted">
          {label}
        </Text>
        <Heading size={useBreakpointValue({ base: 'sm', md: 'md' })} color={color}>
          {value}
        </Heading>
      </Stack>
    </Box>
  );
};

export function Home() {
  const { jobs, jobsLoading } = useJobs();
  const navigate = useNavigate();

  const { tourActive, tourStep, setTourStep, disableTour } = useContext(TourContext);

  useEffect(() => {
    if (tourActive) {
      setTourStep(TourSteps.WelcomeModal);
    }
  }, []);

  let runningJobs = 0;
  let allJobs = 0;
  let failedJobs = 0;

  if (!jobs || jobsLoading) {
    return <Loading />;
  }

  if (jobs) {
    runningJobs = jobs.filter(
      j => j.state == 'Running' || j.state == 'Checkpointing' || j.state == 'Compacting'
    ).length;
    allJobs = jobs.length;
    failedJobs = jobs.filter(j => j.state == 'Failed').length;
  }

  const createPipelineButton = (
    <Popover
      isOpen={tourStep == TourSteps.CreatePipelineButton}
      closeOnBlur={false}
      variant={'tour'}
    >
      <PopoverTrigger>
        <Button
          variant="primary"
          onClick={() => {
            navigate('/pipelines/new');
            setTourStep(TourSteps.CreatePipelineModal);
          }}
        >
          Create Pipeline
        </Button>
      </PopoverTrigger>
      <PopoverContent overflow={'unset'}>
        <PopoverArrow />
        <PopoverCloseButton onClick={disableTour} />
        <PopoverHeader>Create Pipeline</PopoverHeader>
        <PopoverBody>Click here to start creating a pipeline.</PopoverBody>
      </PopoverContent>
    </Popover>
  );

  return (
    <Container py="8" flex="1">
      <WelcomeModal />
      <Stack
        spacing={{
          base: '8',
          lg: '6',
        }}
      >
        <Stack
          spacing="4"
          direction={{
            base: 'column',
            lg: 'row',
          }}
          justify="space-between"
          align={{
            base: 'start',
            lg: 'center',
          }}
        >
          <Stack spacing="1">
            <Heading size="sm" fontWeight="medium">
              Dashboard
            </Heading>
          </Stack>
          <HStack spacing="3">{createPipelineButton}</HStack>
        </Stack>
        <Stack
          spacing={{
            base: '5',
            lg: '6',
          }}
        >
          <SimpleGrid
            columns={{
              base: 1,
              md: 3,
            }}
            gap="6"
          >
            <Stat label="Running Jobs" value={runningJobs?.toString()} />
            <Stat label="All Jobs" value={allJobs?.toString()} />
            <Stat
              label="Failed Jobs"
              value={failedJobs?.toString()}
              color={failedJobs != null && failedJobs > 0 ? 'red.300' : undefined}
            />
          </SimpleGrid>
        </Stack>
      </Stack>
    </Container>
  );
}
