import App from './App';
import './index.css';

import { extendTheme, ThemeConfig } from '@chakra-ui/react';
import { theme as proTheme } from '@chakra-ui/pro-theme';
import { createBrowserRouter, RouterProvider } from 'react-router-dom';
import { PipelinesIndex } from './routes/pipelines/PipelinesIndex';
import { PipelineDetails } from './routes/pipelines/PipelineDetails';
import '@fontsource/inter/variable.css';
import { Home } from './routes/home/Home';
import { Connections } from './routes/connections/Connections';
import { CreatePipeline } from './routes/pipelines/CreatePipeline';

import { addCloudRoutes, createRoot, needsOrgSetup } from './lib/CloudComponents';
import PageNotFound from './routes/not_found/PageNotFound';
import { ChooseConnector } from './routes/connections/ChooseConnector';
import { CreateConnection } from './routes/connections/CreateConnection';

const config: ThemeConfig = {
  initialColorMode: 'dark',
  useSystemColorMode: false,
};

export function Router(): JSX.Element {
  let routes = [
    {
      path: '*',
      element: <PageNotFound />,
    },
    {
      path: '',
      element: <Home />,
    },
    {
      path: 'connections',
      element: <Connections />,
    },
    {
      path: 'connections/new',
      element: <ChooseConnector />,
    },
    {
      path: 'connections/new/:connectorId',
      element: <CreateConnection />,
    },
    {
      path: 'pipelines',
      element: <PipelinesIndex />,
    },
    {
      path: 'pipelines/new',
      element: <CreatePipeline />,
    },
    {
      path: 'pipelines/:pipelineId',
      element: <PipelineDetails />,
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
