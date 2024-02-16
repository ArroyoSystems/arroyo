import React, { useContext, useRef } from 'react';
import { GlobalUdf } from '../../lib/data_fetching';
import {
  Box,
  Button,
  Code,
  Flex,
  Icon,
  IconButton,
  Popover,
  PopoverArrow,
  PopoverBody,
  PopoverContent,
  PopoverHeader,
  PopoverTrigger,
  Text,
  useDisclosure,
} from '@chakra-ui/react';
import SyntaxHighlighter from 'react-syntax-highlighter';
import { vs2015 } from 'react-syntax-highlighter/dist/esm/styles/hljs';
import { AiOutlineDelete } from 'react-icons/ai';
import { LocalUdf, LocalUdfsContext, nameRoot } from '../../udf_state';
import DeleteUdfModal from './DeleteUdfModal';
import UdfLabel from './UdfLabel';

export interface UdfNodePopoverProps {
  udf: GlobalUdf | LocalUdf;
}

const UdfNodePopover: React.FC<UdfNodePopoverProps> = ({ udf }) => {
  const { openTab, isOverridden } = useContext(LocalUdfsContext);
  const {
    isOpen: deleteModalIsOpen,
    onOpen: deleteModalOnOpen,
    onClose: deleteModalOnClose,
  } = useDisclosure();
  const cancelDeleteRef = useRef<HTMLButtonElement>(null);

  const deleteModal = (
    <DeleteUdfModal
      udf={udf}
      deleteModalIsOpen={deleteModalIsOpen}
      deleteModalOnClose={deleteModalOnClose}
      cancelDeleteRef={cancelDeleteRef}
    />
  );

  const deleteButton = (
    <IconButton
      variant={'ghost'}
      aria-label={'Delete'}
      icon={<Icon as={AiOutlineDelete} />}
      onClick={deleteModalOnOpen}
    />
  );

  let warningText = <></>;
  if (isOverridden(udf)) {
    warningText = (
      <Text fontSize={'xs'}>
        This UDF is also defined locally. The local definition will be used.
      </Text>
    );
  }

  const trigger = (
    <PopoverTrigger>
      <Button
        onClick={() => openTab(udf)}
        variant={'ghost'}
        w={'100%'}
        justifyContent={'flex-start'}
        height={'min-content'}
        p={1}
        display={'grid'}
        gridTemplateColumns={'minmax(0, 1fr) min-content'}
      >
        <Flex alignItems={'center'}>
          <UdfLabel udf={udf} />
        </Flex>
      </Button>
    </PopoverTrigger>
  );

  const header = (
    <PopoverHeader display={'flex'} justifyContent={'space-between'} alignItems={'center'}>
      <Code
        borderRadius={3}
        fontSize={'lg'}
        textDecoration={isOverridden(udf) ? 'line-through' : 'none'}
      >
        {nameRoot(udf.name)}
      </Code>
      {warningText}
      {deleteButton}
    </PopoverHeader>
  );

  const definition = (
    <Box minHeight={0}>
      <SyntaxHighlighter
        language="rust"
        style={vs2015}
        customStyle={{
          borderRadius: '5px',
          maxHeight: '100%',
          width: '100%',
          padding: 16,
        }}
      >
        {udf.definition}
      </SyntaxHighlighter>
    </Box>
  );

  return (
    <>
      <Popover placement={'right'} trigger={'hover'}>
        {trigger}
        <PopoverContent width={650} maxHeight={'50vh'} mt={50}>
          <PopoverArrow />
          {header}
          <PopoverBody maxHeight={'500px'} overflow={'auto'}>
            <Text fontSize={'sm'} py={2}>
              {(udf as GlobalUdf).description ?? ''}
            </Text>
            {definition}
          </PopoverBody>
        </PopoverContent>
      </Popover>
      {deleteModal}
    </>
  );
};

export default UdfNodePopover;
