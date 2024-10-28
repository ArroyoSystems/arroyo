import './App.css';
import {
  Button,
  ButtonProps,
  Divider,
  Flex,
  Grid,
  GridItem,
  HStack,
  Icon,
  IconButton,
  Stack,
  Text,
} from '@chakra-ui/react';

import { Link, Outlet, useLinkClickHandler, useMatch } from 'react-router-dom';
import { FiGitBranch, FiHome, FiLink } from 'react-icons/fi';
import { CloudSidebar, UserProfile } from './lib/CloudComponents';
import { usePing } from './lib/data_fetching';
import ApiUnavailable from './routes/not_found/ApiUnavailable';
import Loading from './components/Loading';
import React, { ReactNode, createContext, useContext, useState } from 'react';
import { getTourContextValue, TourContext } from './tour';
import { getLocalUdfsContextValue, LocalUdfsContext } from './udf_state';
import { ChevronLeftIcon, ChevronRightIcon } from '@chakra-ui/icons';
import { motion } from 'framer-motion';
import useLocalStorage from 'use-local-storage';
import { IconType } from 'react-icons';

function logout() {
  // TODO: also send a request to the server to delete the session
  //clearSession();
  location.reload();
}

interface NavButtonProps extends ButtonProps {
  icon: IconType;
  label: string;
  to?: string;
  collapsed: boolean;
  isActive?: boolean;
}

export const NavButton = (props: NavButtonProps) => {
  const { icon, label, collapsed, ...buttonProps } = props;

  let isActive = props.isActive || useMatch(props.to + '/*');

  let onClick = props.to ? useLinkClickHandler(props.to) : props.onClick;

  return (
    <Button
      variant="ghost"
      justifyContent="start"
      /* @ts-ignore */
      onClick={onClick}
      aria-current={isActive ? 'page' : 'false'}
      title={label}
      {...buttonProps}
    >
      <HStack spacing="3">
        <Icon as={icon} boxSize="4" color="subtle" />
        {!collapsed && <Text fontSize="sm">{label}</Text>}
      </HStack>
    </Button>
  );
};

const Sidebar = ({
  collapsed,
  setCollapsed,
}: {
  collapsed: boolean;
  setCollapsed: (c: boolean) => void;
}) => {
  const { menuItems } = useNavbar();

  return (
    <GridItem className="sidebar" area={'nav'} as={motion.div} layout>
      <Flex as="section" minH="100vh" bg="bg-canvas">
        <Flex
          flex="1"
          bg="bg-surface"
          boxShadow="sm-dark"
          maxW={'xs'}
          py={4}
          px={4}
          justify={'center'}
        >
          <Stack justify="space-between" spacing="1" width="full">
            <Stack spacing="4" shouldWrapChildren>
              <Flex justify={'center'}>
                <Link to={'/'}>
                  <motion.img
                    layout
                    style={{ height: 35 }}
                    src={collapsed ? '/icon.svg' : '/logo.svg'}
                  />
                </Link>
              </Flex>
              <Stack spacing="1">
                <NavButton label="Home" to="/" icon={FiHome} collapsed={collapsed} />
                <NavButton
                  label="Connections"
                  to="connections"
                  icon={FiLink}
                  collapsed={collapsed}
                />
                <NavButton
                  label="Pipelines"
                  to="pipelines"
                  icon={FiGitBranch}
                  collapsed={collapsed}
                />
              </Stack>
              <Divider />
              <Stack>
                <CloudSidebar />
              </Stack>
              <Stack>
                {menuItems.map(item => (
                  <NavButton
                    key={item.label}
                    label={item.label}
                    onClick={item.onClick}
                    icon={item.icon}
                    isActive={item.selected}
                    collapsed={collapsed}
                  />
                ))}
              </Stack>
            </Stack>
            <Stack display={'none'}>
              <Divider />
              <UserProfile />
            </Stack>
            <Flex justify={'right'}>
              <IconButton
                aria-label="collapse sidebar"
                onClick={() => setCollapsed(!collapsed)}
                icon={
                  collapsed ? <ChevronRightIcon boxSize={6} /> : <ChevronLeftIcon boxSize={6} />
                }
              />
            </Flex>
          </Stack>
        </Flex>
      </Flex>
    </GridItem>
  );
};

export type SubnavType = {
  icon: IconType;
  label: string;
  onClick: () => void;
  selected: boolean;
};

interface NavbarContextType {
  menuItems: SubnavType[];
  setMenuItems: (items: SubnavType[]) => void;
}

const NavbarContext = createContext<NavbarContextType | undefined>(undefined);

export const NavbarProvider: React.FC<{ children: ReactNode }> = ({ children }) => {
  const [menuItems, setMenuItems] = useState<SubnavType[]>([]);

  return (
    <NavbarContext.Provider value={{ menuItems, setMenuItems }}>{children}</NavbarContext.Provider>
  );
};

export const useNavbar = () => {
  const context = useContext(NavbarContext);
  if (context === undefined) {
    throw new Error('useNavbar must be used within a NavbarProvider');
  }
  return context;
};

function App() {
  const { ping, pingLoading, pingError } = usePing();
  const tourContextValue = getTourContextValue();
  const localUdfsContextValue = getLocalUdfsContextValue();
  const [collapsed, setCollapsed] = useLocalStorage('sidebar-collapse', false);

  let content = (
    <GridItem className="main" area={'main'} overflow={'auto'}>
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
        <NavbarProvider>
          <Grid
            templateAreas={'"nav main"'}
            gridTemplateColumns={`${collapsed ? '80px' : '175px'}`}
            h="100vh"
          >
            <Sidebar collapsed={collapsed} setCollapsed={setCollapsed} />
            {content}
          </Grid>
        </NavbarProvider>
      </LocalUdfsContext.Provider>
    </TourContext.Provider>
  );
}

export default App;
