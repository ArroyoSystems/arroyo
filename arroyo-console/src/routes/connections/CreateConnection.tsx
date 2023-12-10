import { Link, useNavigate, useParams } from 'react-router-dom';
import {
  Box,
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  Container,
  Heading,
  Stack,
  Step,
  StepIcon,
  StepIndicator,
  StepNumber,
  StepSeparator,
  StepStatus,
  StepTitle,
  Stepper,
  useSteps,
} from '@chakra-ui/react';
import { ConnectionSchema, Connector, useConnectors } from '../../lib/data_fetching';
import { useEffect, useState } from 'react';

import { ConfigureConnection } from './ConfigureConnection';
import { DefineSchema } from './DefineSchema';
import { ConnectionTester } from './ConnectionTester';
import { ConfigureProfile } from './ConfigureProfile';

export type CreateConnectionState = {
  name: string | undefined;
  connectionProfileId: string | null;
  table: any;
  schema: ConnectionSchema | null;
};

export const ConnectionCreator = ({ connector }: { connector: Connector }) => {
  const [state, setState] = useState<CreateConnectionState>({
    name: undefined,
    connectionProfileId: null,
    table: null,
    schema: null,
  });

  const { activeStep, setActiveStep } = useSteps({
    index: 0,
    count: connector.customSchemas ? 3 : 2,
  });

  let steps = [];

  if (connector.connectionConfig) {
    steps.push({
      title: 'Configure profile',
      el: (
        <ConfigureProfile
          connector={connector}
          state={state}
          setState={setState}
          onSubmit={() => {
            setActiveStep(steps.length + 1);
          }}
        />
      ),
    });
  }

  steps.push({
    title: 'Configure table',
    el: (
      <ConfigureConnection
        connector={connector}
        state={state}
        setState={setState}
        onSubmit={() => {
          setActiveStep(steps.length + 1);
        }}
      />
    ),
  });

  if (connector.customSchemas) {
    steps.push({
      title: 'Define schema',
      el: (
        <DefineSchema
          connector={connector}
          state={state}
          setState={setState}
          next={() => {
            setActiveStep(2);
          }}
        />
      ),
    });
  }
  steps.push({
    title: 'Create',
    el: <ConnectionTester connector={connector} state={state} setState={setState} />,
  });

  return (
    <Stack spacing={8}>
      <Stepper index={activeStep}>
        {steps.map((step, index) => (
          <Step
            key={index}
            onClick={() => {
              if (activeStep > index) {
                setActiveStep(index);
              }
            }}
          >
            <StepIndicator>
              <StepStatus
                complete={<StepIcon />}
                incomplete={<StepNumber />}
                active={<StepNumber />}
              />
            </StepIndicator>

            <Box flexShrink="0">
              <StepTitle>{step.title}</StepTitle>
            </Box>

            <StepSeparator />
          </Step>
        ))}
      </Stepper>

      {steps[activeStep].el}
    </Stack>
  );
};

export const CreateConnection = () => {
  let { connectorId } = useParams();
  let { connectors, connectorsLoading } = useConnectors();

  let navigate = useNavigate();

  let connector = connectors?.find(c => c.id === connectorId);

  useEffect(() => {
    if (connectors != null && connector == null) {
      navigate('/connections/new');
    }
  });

  if (connectorsLoading || connector == null) {
    return <></>;
  } else {
    return (
      <Container py="8" flex="1">
        <Stack spacing={8} maxW={800}>
          <Breadcrumb>
            <BreadcrumbItem>
              <BreadcrumbLink as={Link} to="/connections">
                Connections
              </BreadcrumbLink>
            </BreadcrumbItem>
            <BreadcrumbItem>
              <BreadcrumbLink as={Link} to="/connections/new">
                Create Connection
              </BreadcrumbLink>
            </BreadcrumbItem>
            <BreadcrumbItem isCurrentPage>
              <BreadcrumbLink>{connector?.name}</BreadcrumbLink>
            </BreadcrumbItem>
          </Breadcrumb>

          <Stack spacing="4">
            <Heading size="sm">Create {connector?.name} connection</Heading>
          </Stack>

          <ConnectionCreator connector={connector!} />
        </Stack>
      </Container>
    );
  }
};
