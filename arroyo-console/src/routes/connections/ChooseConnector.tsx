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
  Image,
  Link,
  SimpleGrid,
  Spacer,
  Stack,
  Text,
} from '@chakra-ui/react';
import { ApiClient } from '../../main';
import { useNavigate } from 'react-router-dom';
import { useConnectors } from '../../lib/data_fetching';

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
            <SimpleGrid spacing={8} minChildWidth={'300px'} w="100%" gridAutoRows={'1fr'}>
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
                        <Image
                          w={75}
                          h={75}
                          color={'white'}
                          src={`data:image/svg+xml;utf8,${encodeURIComponent(c.icon)}`}
                        />
                        <Heading size="sm">{c.name}</Heading>
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
