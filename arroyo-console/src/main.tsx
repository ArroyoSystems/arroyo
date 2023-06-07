import App from './App';
import './index.css';

import { extendTheme, ThemeConfig } from '@chakra-ui/react';
import { theme as proTheme } from '@chakra-ui/pro-theme';
import { createBrowserRouter, RouterProvider } from 'react-router-dom';
import { Sources } from './routes/sources/Sources';
import { JobsIndex } from './routes/pipelines/JobsIndex';
import { ApiGrpc } from './gen/api_connectweb';
import { PromiseClient } from '@bufbuild/connect-web';
import { JobDetail } from './routes/pipelines/JobDetail';
import { OldCreatePipeline } from './routes/pipelines/OldCreatePipeline';
import '@fontsource/inter/variable.css';
import { Home } from './routes/home/Home';
import { Sinks } from './routes/sinks/Sinks';
import { CreateSource } from './routes/sources/CreateSource';
import { Connections } from './routes/connections/Connections';
import { ConnectionEditor } from './routes/connections/CreateConnection';
import { CreatePipeline } from './routes/pipelines/CreatePipeline';
import { SinkEditor } from './routes/sinks/CreateSink';
import { addCloudRoutes, createRoot, getClient, needsOrgSetup } from './lib/CloudComponents';
import PageNotFound from './routes/not_found/PageNotFound';
import { setupWorker } from 'msw';
import { handlers } from './mocks/handlers';

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
      element: <ConnectionEditor client={client} />,
    },
    {
      path: 'sources',
      element: <Sources client={client} />,
    },
    {
      path: 'sources/new',
      element: <CreateSource client={client} />,
    },
    {
      path: 'sinks',
      element: <Sinks client={client} />,
    },
    {
      path: 'sinks/new',
      element: <SinkEditor client={client} />,
    },
    {
      path: 'jobs',
      element: <JobsIndex client={client} />,
    },
    {
      path: 'pipelines/new',
      element: <CreatePipeline client={client} />,
    },
    {
      path: 'pipelines/newOldEditor',
      element: <OldCreatePipeline client={client} />,
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

if (process.env.NODE_ENV === 'development') {
  const worker = setupWorker(...handlers);
  worker.start();
}

const rootElement = document.getElementById('root');
createRoot(rootElement!, theme);
