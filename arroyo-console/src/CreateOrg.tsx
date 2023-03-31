import { Box, Container, Image, Stack, useBreakpointValue, useColorModeValue } from "@chakra-ui/react";
import { CreateOrganization, useAuth } from "@clerk/clerk-react";
import { clerkAppearance } from "./main";

export function NotAvailable(): JSX.Element {
    return (
      <Container maxW="lg" py={{ base: "12", md: "24" }} px={{ base: "0", sm: "8" }}>
        <Stack spacing="8">
          <Stack spacing="6">
            <Image
                style={{ display: "block", marginLeft: "auto", marginRight: "auto" }}
                width="250px"
                src="/logo.svg"
                alt="Arroyo"
                />

          </Stack>
          <Box
            py={{ base: "0", sm: "8" }}
            px={{ base: "4", sm: "10" }}
            bg={useBreakpointValue({ base: "transparent", sm: "bg-surface" })}
            boxShadow={{ base: "none", sm: useColorModeValue("md", "md-dark") }}
            borderRadius={{ base: "none", sm: "xl" }}
          >
            <Stack spacing="6"></Stack>
          </Box>
        </Stack>
      </Container>
    );

}

export function CreateOrg(): JSX.Element {
    return (
      <Container py={{ base: "12", md: "24" }} >
        <CreateOrganization
          appearance={clerkAppearance}
        />
      </Container>
    );
}


