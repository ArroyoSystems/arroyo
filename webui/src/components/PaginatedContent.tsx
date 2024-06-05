import React, { useEffect, useState } from 'react';
import { Flex, IconButton, Stack } from '@chakra-ui/react';
import { ArrowBackIcon, ArrowForwardIcon } from '@chakra-ui/icons';
import Loading from './Loading';

interface Page {
  data: any[];
  hasMore: boolean;
}

export interface PaginatedContentProps {
  pages: Page[] | undefined;
  loading?: boolean;
  totalPages: number;
  setMaxPages: (maxPages: number) => void;
  content: JSX.Element;
  setCurrentData: (data: any[]) => void;
}

const PaginatedContent: React.FC<PaginatedContentProps> = ({
  pages,
  loading,
  totalPages,
  setMaxPages,
  content,
  setCurrentData,
}) => {
  const [pageNum, setPageNum] = useState<number>(1);
  const currentPage = pages?.length ? pages[pageNum - 1] : undefined;

  useEffect(() => {
    setMaxPages(Math.max(pageNum, totalPages));
    if (currentPage) {
      setCurrentData(currentPage.data);
    }
  }, [currentPage]);

  if (!pages || !pages.length || pages.length != totalPages || loading || !currentPage) {
    return <Loading />;
  }

  let pageButtons = <></>;
  if (currentPage.hasMore || pages.length > 1) {
    pageButtons = (
      <Flex justifyContent={'center'} gap={'5px'}>
        <IconButton
          aria-label="Previous page"
          icon={<ArrowBackIcon />}
          isDisabled={pageNum === 1}
          onClick={() => setPageNum(pageNum - 1)}
        />
        <IconButton
          aria-label="Next Page"
          icon={<ArrowForwardIcon />}
          isDisabled={!currentPage.hasMore}
          onClick={() => setPageNum(pageNum + 1)}
        />
      </Flex>
    );
  }

  return (
    <Stack spacing={'5'}>
      {content}
      {pageButtons}
    </Stack>
  );
};

export default PaginatedContent;
