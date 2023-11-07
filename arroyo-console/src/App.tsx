import './App.css';
import {
  As,
  Button,
  ButtonProps,
  Divider,
  Flex,
  Grid,
  GridItem,
  HStack,
  Icon,
  Stack,
  Text,
} from '@chakra-ui/react';

import { Link, Outlet, useLinkClickHandler, useMatch } from 'react-router-dom';
import { FiGitBranch, FiHome, FiLink } from 'react-icons/fi';
import { CloudSidebar, UserProfile } from './lib/CloudComponents';
import { usePing } from './lib/data_fetching';
import ApiUnavailable from './routes/not_found/ApiUnavailable';
import Loading from './components/Loading';
import React from 'react';
import { getTourContextValue, TourContext } from './tour';
import { getLocalUdfsContextValue, LocalUdfsContext } from './udf_state';

function logout() {
  // TODO: also send a request to the server to delete the session
  //clearSession();
  location.reload();
}

interface NavButtonProps extends ButtonProps {
  icon: As;
  label: string;
  to: string;
}

export const NavButton = (props: NavButtonProps) => {
  const { icon, label, ...buttonProps } = props;

  let isActive = useMatch(props.to + '/*');

  let onClick = useLinkClickHandler(props.to);

  return (
    <Button
      variant="ghost"
      justifyContent="start"
      /* @ts-ignore */
      onClick={onClick}
      aria-current={isActive ? 'page' : 'false'}
      {...buttonProps}
    >
      <HStack spacing="3">
        <Icon as={icon} boxSize="4" color="subtle" />
        <Text fontSize="sm">{label}</Text>
      </HStack>
    </Button>
  );
};

function Sidebar() {
  return (
    <GridItem className="sidebar" area={'nav'}>
      <Flex as="section" minH="100vh" bg="bg-canvas">
        <Flex
          flex="1"
          bg="bg-surface"
          boxShadow="sm-dark"
          maxW={{ base: 'full', sm: 'xs' }}
          py={{ base: '6', sm: '8' }}
          px={{ base: '4', sm: '6' }}
        >
          <Stack justify="space-between" spacing="1" width="full">
            <Stack spacing="4" shouldWrapChildren>
              <Link to={'/'}>
                <img width="160px" src="/logo.svg" />
              </Link>
              <Stack spacing="1">
                <NavButton label="Home" to="/" icon={FiHome} />
                <NavButton label="Connections" to="connections" icon={FiLink} />
                <NavButton label="Pipelines" to="pipelines" icon={FiGitBranch} />
              </Stack>
              <Divider />
              <Stack>
                <CloudSidebar />
              </Stack>
            </Stack>
            <Stack>
              <Divider />
              <UserProfile />
            </Stack>
          </Stack>
        </Flex>
      </Flex>
    </GridItem>
  );
}

function App() {
  const { ping, pingLoading, pingError } = usePing();
  const tourContextValue = getTourContextValue();
  const localUdfsContextValue = getLocalUdfsContextValue();

  let content = (
    <GridItem className="main" area={'main'}>
      {<Outlet />}
    </GridItem>
  );

  if (pingLoading) {
    return <Loading />;
  }

  if (!ping || pingError) {
    content = <ApiUnavailable />;
  }

  return (
    <TourContext.Provider value={tourContextValue}>
      <LocalUdfsContext.Provider value={localUdfsContextValue}>
        <Grid templateAreas={'"nav main"'} gridTemplateColumns={'200px minmax(0, 1fr)'} h="100vh">
          <Sidebar />
          {content}
        </Grid>
      </LocalUdfsContext.Provider>
    </TourContext.Provider>
  );
}

export default App;
