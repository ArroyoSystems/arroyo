import { ChakraProvider } from "@chakra-ui/react";
import {
  createGrpcWebTransport,
  createPromiseClient,
} from "@bufbuild/connect-web";
import { ApiGrpc } from "../gen/api_connectweb";
import { ApiClient, Router } from "../main";
import * as ReactDOM from "react-dom/client";
import React from "react";

export function UserProfile() {
  return (
    <></>
  );
}

export function CloudSidebar() {
   return (
    <></>
   );
}

export function addCloudRoutes(routes: Array<{ path: string; element: JSX.Element }>) {
}

export function needsOrgSetup(): JSX.Element | null {
  return null;
}

export function getClient(): ApiClient {
  const transport = createGrpcWebTransport({
    baseUrl: (window as any).__API_ENDPOINT
  });

  const client = createPromiseClient(ApiGrpc, transport);

  return async () => {
    return client;
  };
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
