import React from 'react';

import { Home } from './routes/home/Home';
import { Connections } from './routes/connections/Connections';
import { ChooseConnector } from './routes/connections/ChooseConnector';
import { CreateConnection } from './routes/connections/CreateConnection';
import { PipelinesIndex } from './routes/pipelines/PipelinesIndex';
import { CreatePipeline } from './routes/pipelines/CreatePipeline';
import { PipelineDetails } from './routes/pipelines/PipelineDetails';
import { addCloudRoutes, needsOrgSetup } from './lib/CloudComponents';
import { createBrowserRouter, RouterProvider } from 'react-router-dom';
import App from './App';
import PageNotFound from './routes/not_found/PageNotFound';

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

  let router = createBrowserRouter(
    [
      {
        path: '/',
        element: App(),
        children: routes,
      },
    ],
    {
      basename: window.__ARROYO_BASENAME,
    }
  );

  let orgSetup = needsOrgSetup();
  if (orgSetup) {
    return orgSetup;
  } else {
    return <RouterProvider router={router} />;
  }
}
