import { ChakraProvider } from '@chakra-ui/react';
import { Router } from '../main';
import * as ReactDOM from 'react-dom/client';
import React from 'react';

export function UserProfile() {
  return <></>;
}

export function CloudSidebar() {
  return <></>;
}

export function addCloudRoutes(routes: Array<{ path: string; element: JSX.Element }>) {}

export function needsOrgSetup(): JSX.Element | null {
  return null;
}

export function createRoot(el: HTMLElement, theme: Record<string, any>) {
  return ReactDOM.createRoot(el).render(
    <React.StrictMode>
      <ChakraProvider theme={theme}>
        <Router />
      </ChakraProvider>
    </React.StrictMode>
  );
}
