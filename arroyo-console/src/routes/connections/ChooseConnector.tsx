import {
  Box,
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  Button,
  Card,
  CardBody,
  CardHeader,
  Container,
  Heading,
  Icon,
  Link,
  SimpleGrid,
  Spacer,
  Stack,
  Text,
} from '@chakra-ui/react';
import { ApiClient } from '../../main';
import { useNavigate } from 'react-router-dom';
import { useConnectors } from '../../lib/data_fetching';

// export const CONNECTORS = {
//   "kafka": {
//     name: 'Kafka',
//     icon: SiApachekafka,
//     description: 'Confluent Cloud, Amazon MSK, or self-hosted',
//     disabled: false,
//     source: true,
//     sink: true,
//   },
//   "pulsar": {
//     name: 'Pulsar',
//     icon: SiApachekafka,
//     description: 'Confluent Cloud, Amazon MSK, or self-hosted',
//     disabled: false,
//     source: true,
//     sink: true,
//   },
//   "redpanda": {
//     name: 'Redpanda',
//     icon: SiApachekafka,
//     description: 'Confluent Cloud, Amazon MSK, or self-hosted',
//     disabled: false,
//     source: true,
//     sink: true,
//   },
//   "kinesis": {
//     name: 'Kinesis',
//     icon: FaStream,
//     description: 'AWS Kinesis stream (coming soon)',
//     disabled: true,
//     source: true,
//     sink: true,
//   },
//   "http": {
//     name: 'HTTP',
//     icon: FaGlobeAmericas,
//     description: 'HTTP/HTTPS server',
//     disabled: false,
//     source: true,
//     sink: false,
//   },
//   "websocket": {
//     name: 'WebSocket',
//     icon: FaGlobeAmericas,
//     description: 'HTTP/HTTPS server',
//     disabled: false,
//     source: true,
//     sink: false,
//   },
//   "deltalake": {
//     name: 'DeltaLake',
//     icon: FaGlobeAmericas,
//     description: 'Delta Lake cluster on S3',
//     disabled: false,
//     source: false,
//     sink: true,
//   },
// };

export function ChooseConnector({ client }: { client: ApiClient }) {
  const navigate = useNavigate();
  const { connectors } = useConnectors(client);

  return (
    <Container py="8" flex="1">
      <Stack spacing={6}>
        <Breadcrumb>
          <BreadcrumbItem>
            <BreadcrumbLink as={Link} href="/connections">
              Connections
            </BreadcrumbLink>
          </BreadcrumbItem>
          <BreadcrumbItem isCurrentPage>
            <BreadcrumbLink>Create Connection</BreadcrumbLink>
          </BreadcrumbItem>
        </Breadcrumb>

        <Stack spacing="4">
          <Heading size="xs">Select Connector</Heading>
          <Text>Connectors enable reading and writing data from external systems</Text>
          <Box p={8}>
            <SimpleGrid spacing={8} minChildWidth={'250px'} w="100%" gridAutoRows={'1fr'}>
              {connectors?.connectors.map(c => {
                return (
                  <Card key={c.name} size={'md'} maxW={400}>
                    <Text
                      fontSize={'12px'}
                      p={2}
                      w={'100%'}
                      bgColor={'gray.600'}
                      color={'blue.100'}
                    >
                      {c.source && c.sink ? 'source / sink' : c.source ? 'source' : 'sink'}
                    </Text>

                    <CardHeader>
                      <Stack direction="row" spacing={4} align="center">
                        <Icon boxSize={7}></Icon>
                        <Heading size="xs">{c.name}</Heading>
                      </Stack>
                    </CardHeader>
                    <CardBody>
                      <Stack h="100%">
                        <Text pb={4}>{c.description}</Text>
                        <Spacer />
                        <Button
                          colorScheme="blue"
                          backgroundColor={'gray.600'}
                          variant={'outline'}
                          onClick={() => navigate(`/connections/new/${c.id}`)}
                        >
                          Create
                        </Button>
                      </Stack>
                    </CardBody>
                  </Card>
                );
              })}
            </SimpleGrid>
          </Box>
        </Stack>
      </Stack>
    </Container>
  );
}
