import {
  Accordion,
  AccordionButton,
  AccordionIcon,
  AccordionItem,
  AccordionPanel,
  Box,
  Flex,
  Text,
} from '@chakra-ui/react';
import { ConnectionTable, SourceField } from '../../lib/data_fetching';

function CatalogField({ field, nesting }: { field: SourceField; nesting: number }) {
  if (field.fieldType!.type.primitive) {
    return (
      <Flex>
        <Box flex="1" textAlign="left">
          {field.fieldName}
        </Box>
        <Box flex="1" textAlign="right">
          {field.fieldType.sqlName}
        </Box>
      </Flex>
    );
  }
  if (field.fieldType!.type.struct) {
    return (
      <Box mr={1} mb={2}>
        <Box>{field.fieldName}</Box>
        {field.fieldType.type.struct.fields.map(f => {
          return (
            <Box pl={(nesting + 1) * 4} key={f.fieldName} fontFamily="monospace">
              <CatalogField field={f} nesting={nesting + 1} />
            </Box>
          );
        })}
      </Box>
    );
  } else {
    return <Box></Box>;
  }
}

export function Catalog({ tables }: { tables: Array<ConnectionTable> }) {
  return (
    <Accordion allowMultiple defaultIndex={[]}>
      {tables.map(table => {
        return (
          <AccordionItem key={table.name} fontSize="xs" pb={4} borderWidth={0}>
            <Box>
              <AccordionButton padding={0}>
                <Box fontWeight="bold" as="span" flex="1" textAlign="left">
                  <Text fontSize="xs" pb={2}>
                    {table.name}
                  </Text>
                </Box>
                <AccordionIcon />
              </AccordionButton>
            </Box>
            <AccordionPanel padding={0} pl={3}>
              {table.schema!.fields.map(f => (
                <CatalogField key={f.fieldName} field={f} nesting={0} />
              ))}
            </AccordionPanel>
          </AccordionItem>
        );
      })}
    </Accordion>
  );
}
