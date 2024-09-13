import React, { useContext } from 'react';
import { Text, Flex, Icon } from '@chakra-ui/react';
import { DiPython, DiRust } from 'react-icons/di';
import { LocalUdf, LocalUdfsContext, nameRoot } from '../../udf_state';
import { GlobalUdf } from '../../lib/data_fetching';
import '@fontsource/ibm-plex-mono';

export interface UdfLabelProps {
  udf: LocalUdf | GlobalUdf;
}

const UdfLabel: React.FC<UdfLabelProps> = ({ udf }) => {
  const { isOverridden, openTab } = useContext(LocalUdfsContext);
  const hasErrors = 'errors' in udf && udf.errors?.length;

  return (
    <Flex alignItems={'center'} onClick={() => openTab(udf)} cursor={'pointer'} w={'min-content'}>
      <Icon as={udf.language == 'rust' ? DiRust : DiPython} boxSize={5} />
      <Text
        borderRadius={3}
        fontSize={'sm'}
        p={1}
        borderBottom={hasErrors ? '2px dotted red' : 'none'}
        textOverflow={'ellipsis'}
        whiteSpace={'normal'}
        noOfLines={1}
        textDecoration={isOverridden(udf) ? 'line-through' : 'none'}
        fontFamily={'IBM Plex Mono, monospace'}
      >
        {nameRoot(udf.name)}
      </Text>
    </Flex>
  );
};

export default UdfLabel;
