import { Dispatch } from "react";
import { CreateConnectionState } from "./CreateConnection";
import { ApiClient } from "../../main";
import { Stack, Text } from "@chakra-ui/react";

export function ConnectionTester({
    state,
    setState,
    next,
    client
}: {
  state: CreateConnectionState;
  setState: Dispatch<CreateConnectionState>;
  next: () => void;
  client: ApiClient;
}) {
    return <Stack spacing={4}>
        <Text>Test</Text>
    </Stack>;
}
