import React, { useContext, useRef, useState } from 'react';
import {
  Button,
  CloseButton,
  Flex,
  Popover,
  PopoverArrow,
  PopoverBody,
  PopoverContent,
  PopoverHeader,
  PopoverTrigger,
  Tab,
  Text,
  useDisclosure,
} from '@chakra-ui/react';
import { AiOutlineDelete, AiOutlineShareAlt } from 'react-icons/ai';
import GlobalizeModal from './GlobalizeModal';
import { post, GlobalUdf } from '../../lib/data_fetching';
import { LocalUdf, LocalUdfsContext } from '../../udf_state';
import DeleteUdfModal from './DeleteUdfModal';
import UdfLabel from './UdfLabel';

export interface UdfEditTabProps {
  udf: LocalUdf | GlobalUdf;
}

const UdfEditTab: React.FC<UdfEditTabProps> = ({ udf }) => {
  const { localUdfs, setLocalUdfs, closeUdf, isGlobal } = useContext(LocalUdfsContext);
  const [loading, setLoading] = useState<boolean>(false);
  const {
    isOpen: globalizeModalIsOpen,
    onOpen: globalizeModalOnOpen,
    onClose: globalizeModalOnClose,
  } = useDisclosure();
  const {
    isOpen: deleteModalIsOpen,
    onOpen: deleteModalOnOpen,
    onClose: deleteModalOnClose,
  } = useDisclosure();
  const cancelDeleteRef = useRef<HTMLButtonElement>(null);

  const tab = (
    <Tab px={2} key={udf.id}>
      <UdfLabel udf={udf} />
      <CloseButton as={'div'} w={5} fontSize={8} onClick={() => closeUdf(udf)} />
    </Tab>
  );

  const globalizeModal = (
    <GlobalizeModal
      isOpen={globalizeModalIsOpen}
      onClose={globalizeModalOnClose}
      udf={udf as LocalUdf}
    />
  );

  const deleteModal = (
    <DeleteUdfModal
      udf={udf}
      deleteModalIsOpen={deleteModalIsOpen}
      deleteModalOnClose={deleteModalOnClose}
      cancelDeleteRef={cancelDeleteRef}
    />
  );

  const handleShare = async () => {
    // synchronously validate udf
    setLoading(true);
    const { data: udfValiation, error: udfValiationError } = await post('/v1/udfs/validate', {
      body: {
        definition: udf.definition,
      },
    });
    setLoading(false);

    if (udfValiation) {
      const newUdfs = localUdfs.map(u => {
        if (u.id == udf.id) {
          u.errors = udfValiation.errors || [];
        }
        return u;
      });
      setLocalUdfs(newUdfs);
    }

    globalizeModalOnOpen();
  };

  let title;
  if (localUdfs.some(u => u.id == udf.id)) {
    title = 'Local UDF';
  } else {
    title = 'Global UDF';
  }

  let globalizeButton = <></>;
  if (!isGlobal(udf)) {
    const buttonText = 'Globalize';
    globalizeButton = (
      <Button
        leftIcon={<AiOutlineShareAlt />}
        onClick={handleShare}
        isLoading={loading}
        loadingText={buttonText}
      >
        {buttonText}
      </Button>
    );
  }

  const popoverBody = (
    <Flex gap={2}>
      {globalizeButton}
      <Button leftIcon={<AiOutlineDelete />} onClick={deleteModalOnOpen}>
        Delete
      </Button>
    </Flex>
  );

  return (
    <>
      <Popover trigger={'hover'} variant={'default'} closeDelay={0} gutter={-8}>
        <PopoverTrigger>{tab}</PopoverTrigger>
        <PopoverContent width={'min-content'}>
          <PopoverArrow />
          <PopoverHeader>
            <Text align={'center'} fontStyle={'italic'} fontSize={'sm'}>
              {title}
            </Text>
          </PopoverHeader>
          <PopoverBody>{popoverBody}</PopoverBody>
        </PopoverContent>
      </Popover>
      {globalizeModal}
      {deleteModal}
    </>
  );
};

export default UdfEditTab;
