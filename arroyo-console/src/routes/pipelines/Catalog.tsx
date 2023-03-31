import {
  Accordion,
  AccordionButton,
  AccordionIcon,
  AccordionItem,
  AccordionPanel,
  Box,
  Flex,
  Text,
} from "@chakra-ui/react";
import { SourceDef, SourceField, PrimitiveType } from "../../gen/api_pb";

function CatalogField({ field, nesting }: { field: SourceField; nesting: number }) {
  switch (field.fieldType!.type.case) {
    case "primitive":
      return (
        <Flex>
          <Box flex="1" textAlign="left">
            {field.fieldName}
          </Box>
          <Box flex="1" textAlign="right">
            {PrimitiveType[field.fieldType!.type.value]}
          </Box>
        </Flex>
      );
    case "struct":
      return (
        <Box mr={1} mb={2}>
          <Box>{field.fieldName}</Box>
          {field.fieldType!.type.value.fields.map(f => {
            return (
              <Box pl={(nesting + 1) * 4} key={f.fieldName} fontFamily="monospace">
                <CatalogField field={f} nesting={nesting + 1} />
              </Box>
            );
          })}
        </Box>
      );
    default:
      return <Box></Box>;
  }
}

export function Catalog({ sources }: { sources: Array<SourceDef> }) {
  return (
    <Accordion allowMultiple defaultIndex={[]}>
      {sources.map(source => {
        return (
          <AccordionItem key={source.name} fontSize="xs" pb={4}>
            <Box>
              <AccordionButton padding={0}>
                <Box fontWeight="bold" as="span" flex="1" textAlign="left">
                  <Text fontSize="xs" pb={2}>
                    {source.name}
                  </Text>
                </Box>
                <AccordionIcon />
              </AccordionButton>
            </Box>
            <AccordionPanel padding={0} pl={3}>
              {source.sqlFields.map(f => (
                <CatalogField key={f.fieldName} field={f} nesting={0} />
              ))}
            </AccordionPanel>
          </AccordionItem>
        );
      })}
    </Accordion>
  );
}
