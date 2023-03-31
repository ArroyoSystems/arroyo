import {
  Container,
  Stack,
  Heading,
  HStack,
  Button,
  useColorModeValue,
  Box,
} from "@chakra-ui/react";
import { OrganizationProfile, UserProfile } from "@clerk/clerk-react";
import { clerkAppearance } from "../../main";

export function OrganizationPage() {
  return (
    <Container py="8" flex="1" className="settings">
      <Stack spacing={{ base: "8", lg: "6" }}>
        <Stack
          spacing="4"
          direction={{ base: "column", lg: "row" }}
          justify="space-between"
          align={{ base: "start", lg: "center" }}
        >
          <Stack spacing="1">
            <Heading size="sm" fontWeight="medium">
              Settings
            </Heading>
          </Stack>
        </Stack>
        <Box
          bg="bg-surface"
          boxShadow={{ base: "none", md: useColorModeValue("sm", "sm-dark") }}
          borderRadius="lg"
        >
          <OrganizationProfile appearance={clerkAppearance} />
        </Box>
      </Stack>
    </Container>
  );
}
