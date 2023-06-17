import { Link, useNavigate, useParams } from 'react-router-dom';
import { ApiClient } from '../../main';
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
import { useConnectors } from '../../lib/data_fetching';
import { useEffect, useState } from 'react';
import { ConnectionSchema, Connector, SourceSchema } from '../../gen/api_pb';

import { ConfigureConnection } from './ConfigureConnection';
import { DefineSchema } from './DefineSchema';
import { ConnectionTester } from './ConnectionTester';

export type CreateConnectionState = {
  connectionId: string | null;
  table: any;
  schema: ConnectionSchema | null;
}

export const ConnectionCreator = ({
  client,
  connector,
}: {
  client: ApiClient;
  connector: Connector;
}) => {
  const [state, setState] = useState<CreateConnectionState>({
    connectionId: null,
    table: null,
    schema: null,
  });

  const { activeStep, setActiveStep } = useSteps({
    index: 0,
    count: connector.customSchemas ? 3 : 2,
  });

  let steps = [
    {
      title: 'Configure connection',
      el: (
        <ConfigureConnection
          client={client}
          connector={connector}
          state={state}
          setState={setState}
          onSubmit={() => {
            setActiveStep(1);
          }}
        />
      ),
    },
    {
      title: 'Define schema',
      el: <DefineSchema
        client={client}
        connector={connector}
        state={state}
        setState={setState}
        next={() => {
          setActiveStep(2);
        }}
       />
    },
    {
      title: 'Test' ,
      el: <ConnectionTester
        client={client}
        connector={connector}
        state={state}
        setState={setState}
        next={() => {}}
        />
    },
  ];

  if (!connector!.customSchemas) {
    steps.splice(1, 1);
  }

  return (
    <Stack spacing={8}>
      <Stepper index={activeStep}>
        {steps.map((step, index) => (
          <Step key={index} onClick={() => {
            if (activeStep > index) {
              setActiveStep(index);
            }
          }}>
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

export const CreateConnection = ({ client }: { client: ApiClient }) => {
  let { connectorId } = useParams();
  let { connectors, connectorsLoading } = useConnectors(client);

  let navigate = useNavigate();

  let connector = connectors?.connectors.find(c => c.id === connectorId);

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

          <ConnectionCreator
            client={client}
            connector={connector!}
            />
        </Stack>
      </Container>
    );
  }
};
