import React, { useContext } from 'react';
import { Code, Flex, Icon } from '@chakra-ui/react';
import { DiRust } from 'react-icons/di';
import { LocalUdf, LocalUdfsContext, nameRoot } from '../../udf_state';
import { GlobalUdf } from '../../lib/data_fetching';

export interface UdfLabelProps {
  udf: LocalUdf | GlobalUdf;
}

const UdfLabel: React.FC<UdfLabelProps> = ({ udf }) => {
  const { isOverridden, openTab } = useContext(LocalUdfsContext);
  const hasErrors = 'errors' in udf && udf.errors?.length;

  return (
    <Flex alignItems={'center'} onClick={() => openTab(udf)} cursor={'pointer'} w={'min-content'}>
      <Icon as={DiRust} boxSize={5} />
      <Code
        borderRadius={3}
        fontSize={'md'}
        borderBottom={hasErrors ? '2px dotted red' : 'none'}
        textOverflow={'ellipsis'}
        whiteSpace={'normal'}
        noOfLines={1}
        textDecoration={isOverridden(udf) ? 'line-through' : 'none'}
      >
        {nameRoot(udf.name)}
      </Code>
    </Flex>
  );
};

export default UdfLabel;
