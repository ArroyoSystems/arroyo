import React, { useContext, useState } from 'react';
import { useGlobalUdfs, GlobalUdf } from '../../lib/data_fetching';
import UdfNodeViewer from '../../components/UdfNodeViewer';
import {
  Accordion,
  AccordionButton,
  AccordionIcon,
  AccordionItem,
  AccordionPanel,
  Box,
  Button,
  Flex,
  Text,
} from '@chakra-ui/react';
import { LocalUdf, LocalUdfsContext } from '../../udf_state';

export interface UdfsTabProps {}

export type UdfNode = UdfFolder | GlobalUdf | LocalUdf;

export interface UdfFolder {
  id: string;
  folderName: string;
  children: UdfNode[];
}

const UdfsResourceTab: React.FC<UdfsTabProps> = () => {
  const { localUdfs, newUdf } = useContext(LocalUdfsContext);
  const { globalUdfs } = useGlobalUdfs();
  const [accordionState, setAccordionState] = useState<number[]>([0, 1]);

  const udfTree: UdfNode = { id: '/', folderName: '/', children: [] };

  if (globalUdfs) {
    globalUdfs.forEach(udf => {
      let splitPath = udf.prefix.split('/').filter(s => s != '');
      splitPath.push(udf.name);
      let currentFolder = udfTree;
      splitPath.forEach((pathBit, i) => {
        if (i == splitPath.length - 1) {
          currentFolder.children.push(udf);
        } else {
          let folder = currentFolder.children.find(
            child => (child as UdfFolder).folderName == pathBit
          );
          if (!folder) {
            folder = { id: pathBit, folderName: pathBit, children: [] };
            currentFolder.children.push(folder);
          }
          currentFolder = folder as UdfFolder;
        }
      });
    });
  }

  // sort by folder first, then by udf name
  udfTree.children.sort((a, b) => {
    if ('definition' in a) {
      return 1;
    }
    if ('definition' in b) {
      return -1;
    }
    return a.folderName.localeCompare(b.folderName);
  });

  const local = (
    <AccordionItem key={'local'} borderTop={'none'}>
      <AccordionButton px={0} display={'flex'} justifyContent={'space-between'}>
        <Flex flexDirection={'column'} gap={2}>
          <Text fontWeight={'bold'} align={'left'}>
            LOCAL
          </Text>
          <Text align={'left'} fontSize={'sm'}>
            These UDFs can be used only by this pipeline.
          </Text>
        </Flex>
        <AccordionIcon />
      </AccordionButton>
      <AccordionPanel px={0} display={'flex'} flexDirection={'column'} gap={2}>
        <Box>
          {localUdfs.map(udf => (
            <UdfNodeViewer key={udf.id} node={udf} />
          ))}
        </Box>
        <Box>
          <Button height={7} onClick={newUdf} width={'100%'}>
            New
          </Button>
        </Box>
      </AccordionPanel>
    </AccordionItem>
  );

  const shared = (
    <AccordionItem key={'shared'} borderBottom={'none'}>
      <AccordionButton px={0} display={'flex'} justifyContent={'space-between'}>
        <Flex flexDirection={'column'} gap={2}>
          <Text align={'left'} fontWeight={'bold'}>
            GLOBAL
          </Text>
          <Text align={'left'} fontSize={'sm'}>
            These UDFs are shared across all pipelines.
          </Text>
        </Flex>
        <AccordionIcon />
      </AccordionButton>
      <AccordionPanel px={0} display={'flex'} flexDirection={'column'} gap={2}>
        <div>
          {udfTree.children.map(child => (
            <UdfNodeViewer key={child.id} node={child} />
          ))}
        </div>
      </AccordionPanel>
    </AccordionItem>
  );

  return (
    <Flex flexDirection={'column'} gap={3}>
      <Accordion
        allowMultiple
        index={accordionState}
        onChange={(state: number[]) => setAccordionState(state)}
      >
        {local}
        {shared}
      </Accordion>
    </Flex>
  );
};

export default UdfsResourceTab;
