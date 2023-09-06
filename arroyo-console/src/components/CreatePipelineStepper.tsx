import {
  Box,
  Step,
  StepIcon,
  StepIndicator,
  Stepper,
  StepSeparator,
  StepStatus,
  StepTitle,
  StepDescription,
  Text,
  Badge,
  Flex,
} from '@chakra-ui/react';
import React from 'react';
import { Job, PipelineGraph } from '../lib/data_fetching';

export interface CreatePipelineStepperProps {
  pipelineGraph?: PipelineGraph;
  pipelineGraphError?: any;
  job?: Job;
  modalOpen: boolean;
}

const CreatePipelineStepper: React.FC<CreatePipelineStepperProps> = ({
  pipelineGraph,
  pipelineGraphError,
  job,
  modalOpen,
}) => {
  let activeStep = 0;

  let checkBadge = <></>;
  if (pipelineGraph) {
    checkBadge = <Badge colorScheme={'green'}>Passed</Badge>;
    activeStep = 1;
  }
  if (pipelineGraphError) {
    checkBadge = <Badge colorScheme={'red'}>Failed</Badge>;
  }

  let previewBadge = <></>;
  if (job?.state) {
    previewBadge = <Badge>{job.state}</Badge>;
    if (job.state === 'Failed') {
      previewBadge = <Badge colorScheme={'red'}>Failed</Badge>;
    }
  }
  if (job?.startTime && job?.finishTime && !job?.failureMessage) {
    activeStep = 2;
    previewBadge = <Badge colorScheme={'green'}>Completed</Badge>;
  }

  let createBadge = <></>;
  if (modalOpen) {
    activeStep = 2;
    createBadge = <Badge>In progress</Badge>;

    if (!job) {
      previewBadge = <Badge>Skipped</Badge>;
    }
  }

  const steps = [
    {
      title: 'Check',
      description: 'Validate query and view pipeline graph',
      status: checkBadge,
    },
    {
      title: 'Preview',
      description: 'Preview job to see results (optional)',
      status: previewBadge,
    },
    { title: 'Create', description: 'Launch pipeline', status: createBadge },
  ];

  return (
    <Box margin={4}>
      <Stepper size={'sm'} index={activeStep}>
        {steps.map((step, index) => (
          <Step key={index}>
            <StepIndicator>
              <StepStatus complete={<StepIcon />} />
            </StepIndicator>
            <Box>
              <StepTitle>
                <Flex gap={2}>
                  <Text fontSize={'sm'}>{step.title}</Text>
                  {step.status}
                </Flex>
              </StepTitle>
              <StepDescription>
                <Text fontSize={'xs'} noOfLines={2}>
                  {step.description}
                </Text>
              </StepDescription>
            </Box>
            <Flex minWidth={10} flex={1}>
              <StepSeparator />
            </Flex>
          </Step>
        ))}
      </Stepper>
    </Box>
  );
};

export default CreatePipelineStepper;
