import {
  Accordion,
  AccordionButton,
  AccordionIcon,
  AccordionItem,
  AccordionPanel,
  Box,
  Flex,
  HStack,
  Icon,
  Text,
} from '@chakra-ui/react';
import { ConnectionTable, SourceField } from '../../lib/data_fetching';
import { BiTable } from 'react-icons/bi';

function CatalogField({ field, nesting }: { field: SourceField; nesting: number }) {
  // @ts-ignore
  if (field.type == 'struct') {
    return (
      <Box mr={1} mb={2}>
        <Box>{field.name}</Box>
        {
          // @ts-ignore
          field.fields.map(f => {
            return (
              <Box pl={(nesting + 1) * 4} key={f.fieldName} fontFamily="monospace">
                <CatalogField field={f} nesting={nesting + 1} />
              </Box>
            );
          })
        }
      </Box>
    );
  }

  return (
    <Flex>
      <Box flex="1" textAlign="left">
        {field.name}
      </Box>
      <Box flex="1" textAlign="right" color={'pink.300'}>
        {field.sqlName}
      </Box>
    </Flex>
  );
}

export function Catalog({ tables }: { tables: Array<ConnectionTable> }) {
  return (
    <Accordion allowMultiple defaultIndex={[]}>
      {tables.map(table => {
        return (
          <AccordionItem key={table.name} fontSize="xs" pb={4} borderWidth={0}>
            <Box>
              <AccordionButton padding={0}>
                <HStack
                  fontWeight="bold"
                  as="span"
                  flex="1"
                  textAlign="left"
                  fontFamily={'monospace'}
                  alignItems={'middle'}
                  color={'gray.300'}
                >
                  <Icon as={BiTable} boxSize={'4'} color={'blue.400'} />
                  <Text fontSize="12px" pb={2}>
                    {table.name}
                  </Text>
                </HStack>
                <AccordionIcon />
              </AccordionButton>
            </Box>
            <AccordionPanel padding={0} pl={3}>
              {table.schema!.fields?.map(f => (
                <CatalogField key={f.name} field={f} nesting={0} />
              ))}
            </AccordionPanel>
          </AccordionItem>
        );
      })}
    </Accordion>
  );
}
