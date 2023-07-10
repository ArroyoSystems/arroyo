import App from './App';
import './index.css';

import { extendTheme, ThemeConfig } from '@chakra-ui/react';
import { theme as proTheme } from '@chakra-ui/pro-theme';
import { createBrowserRouter, RouterProvider } from 'react-router-dom';
import { JobsIndex } from './routes/pipelines/JobsIndex';
import { ApiGrpc } from './gen/api_connectweb';
import { PromiseClient } from '@bufbuild/connect-web';
import { JobDetail } from './routes/pipelines/JobDetail';
import '@fontsource/inter/variable.css';
import { Home } from './routes/home/Home';
import { Connections } from './routes/connections/Connections';
import { CreatePipeline } from './routes/pipelines/CreatePipeline';

import { addCloudRoutes, createRoot, getClient, needsOrgSetup } from './lib/CloudComponents';
import PageNotFound from './routes/not_found/PageNotFound';
import { ChooseConnector } from './routes/connections/ChooseConnector';
import { CreateConnection } from './routes/connections/CreateConnection';

export type ApiClient = () => Promise<PromiseClient<typeof ApiGrpc>>;

const config: ThemeConfig = {
  initialColorMode: 'dark',
  useSystemColorMode: false,
};

export function Router(): JSX.Element {
  const client = getClient();

  let routes = [
    {
      path: '*',
      element: <PageNotFound />,
    },
    {
      path: '',
      element: <Home client={client} />,
    },
    {
      path: 'connections',
      element: <Connections client={client} />,
    },
    {
      path: 'connections/new',
      element: <ChooseConnector client={client} />,
    },
    {
      path: 'connections/new/:connectorId',
      element: <CreateConnection client={client} />,
    },
    {
      path: 'pipelines',
      element: <JobsIndex client={client} />,
    },
    {
      path: 'pipelines/new',
      element: <CreatePipeline client={client} />,
    },
    {
      path: 'jobs/:id',
      element: <JobDetail client={client} />,
    },
  ];

  addCloudRoutes(routes);

  let router = createBrowserRouter([
    {
      path: '/',
      element: App(),
      children: routes,
    },
  ]);

  let orgSetup = needsOrgSetup();
  if (orgSetup) {
    return orgSetup;
  } else {
    return <RouterProvider router={router} />;
  }
}

const theme = extendTheme(proTheme, {
  colors: { ...proTheme.colors, brand: proTheme.colors.blue },
  config: config,
});

const rootElement = document.getElementById('root');
createRoot(rootElement!, theme);
