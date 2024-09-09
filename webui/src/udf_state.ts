import React, { useEffect, useState } from 'react';
import useLocalStorage from 'use-local-storage';
import { GlobalUdf, useGlobalUdfs } from './lib/data_fetching';
import { generate_udf_id } from './lib/util';

export interface LocalUdf {
  name: string;
  definition: string;
  language: 'python' | 'rust';
  id: string;
  open: boolean;
  errors?: string[];
}

export const LocalUdfsContext = React.createContext<{
  localUdfs: LocalUdf[];
  setLocalUdfs: (localUdfs: LocalUdf[]) => void;
  deleteLocalUdf: (udf: LocalUdf) => void;
  deleteGlobalUdf: (udf: GlobalUdf) => void;
  deleteUdf: (udf: LocalUdf | GlobalUdf) => void;
  openedUdfs: (GlobalUdf | LocalUdf)[];
  openTab: (tabItem: 'query' | LocalUdf | GlobalUdf) => void;
  closeUdf: (udf: LocalUdf | GlobalUdf) => void;
  isOverridden: (udf: LocalUdf | GlobalUdf) => boolean;
  updateLocalUdf: (
    udf: LocalUdf,
    update: { definition?: string; open?: boolean; name?: string }
  ) => void;
  isGlobal: (udf: LocalUdf | GlobalUdf) => boolean;
  newUdf: (language: 'python' | 'rust') => void;
  editorTab: number;
  handleEditorTabChange: (index: number) => void;
}>({
  localUdfs: [],
  setLocalUdfs: _ => {},
  deleteUdf: _ => {},
  deleteLocalUdf: _ => {},
  deleteGlobalUdf: _ => {},
  openedUdfs: [],
  openTab: _ => {},
  closeUdf: _ => {},
  isOverridden: _ => false,
  updateLocalUdf: (_, __) => {},
  isGlobal: _ => false,
  newUdf: (language: 'python' | 'rust') => {},
  editorTab: 0,
  handleEditorTabChange: _ => {},
});

export const getLocalUdfsContextValue = () => {
  const [localUdfs, setLocalUdfs] = useLocalStorage<LocalUdf[]>('localUdfs', []);
  const { globalUdfs, deleteGlobalUdf: apiDeleteGlobalUdf } = useGlobalUdfs();
  const [openedGlobalUdfs, setOpenedGlobalUdfs] = useState<GlobalUdf[]>([]);
  const [editorTab, setEditorTab] = useState(0);
  const [selectedUdfId, setSelectedUdfId] = useState<string | undefined>('');

  const openedUdfs = [...localUdfs.filter(u => u.open), ...openedGlobalUdfs];
  const openedUdfsIds = openedUdfs.map(u => u.id);

  useEffect(() => {
    const i = openedUdfsIds.findIndex(u => u === selectedUdfId);
    setEditorTab(i + 1);
  }, [selectedUdfId, openedUdfsIds]);

  const openTab = (tabItem: 'query' | LocalUdf | GlobalUdf) => {
    if (tabItem === 'query') {
      setSelectedUdfId(undefined);
      return;
    }

    if (isGlobal(tabItem) && !openedGlobalUdfs.includes(tabItem as GlobalUdf)) {
      setOpenedGlobalUdfs([...openedGlobalUdfs, tabItem as GlobalUdf]);
    }
    setLocalUdfs(localUdfs.map(u => (u.id === tabItem.id ? { ...u, open: true } : u)));
    setSelectedUdfId(tabItem.id);
  };

  const handleEditorTabChange = (index: number) => {
    if (index === 0) {
      setSelectedUdfId(undefined);
    } else {
      setSelectedUdfId(openedUdfsIds[index - 1]);
    }
    setEditorTab(index);
  };

  const closeUdf = (udf: LocalUdf | GlobalUdf) => {
    if (globalUdfs && globalUdfs.some(g => g.id === udf.id)) {
      setOpenedGlobalUdfs(openedGlobalUdfs.filter(u => u.id !== udf.id));
    }
    setLocalUdfs(localUdfs.map(u => (u.id === udf.id ? { ...u, open: false } : u)));
  };

  const deleteLocalUdf = (udf: LocalUdf) => {
    const newLocalUdfs = localUdfs.filter(u => u.id !== udf.id);
    setLocalUdfs(newLocalUdfs);
  };

  const deleteGlobalUdf = async (udf: GlobalUdf) => {
    await apiDeleteGlobalUdf(udf);
    setOpenedGlobalUdfs(openedGlobalUdfs.filter(u => u.id !== udf.id));
  };

  const deleteUdf = async (udf: LocalUdf | GlobalUdf) => {
    if (globalUdfs && globalUdfs.some(g => g.id === udf.id)) {
      await deleteGlobalUdf(udf as GlobalUdf);
    }
    setLocalUdfs(localUdfs.filter(u => u.id !== udf.id));
  };

  const isOverridden = (udf: LocalUdf | GlobalUdf) => {
    if (globalUdfs && globalUdfs.some(u => u.id == udf.id)) {
      if (localUdfs.length && localUdfs.some(u => u.name == nameRoot(udf.name))) {
        return true;
      }
    }
    return false;
  };

  const updateLocalUdf = (
    udf: LocalUdf,
    update: { definition?: string; open?: boolean; name?: string }
  ) => {
    setLocalUdfs(localUdfs.map(u => (u.id === udf.id ? { ...u, ...update } : u)));
  };

  const isGlobal = (udf: LocalUdf | GlobalUdf) => {
    return globalUdfs != undefined && globalUdfs.some(g => g.id === udf.id);
  };

  const newUdf = (language: 'python' | 'rust') => {
    const id = generate_udf_id();
    const functionName = `new_udf`;

    const defaultRust = `
/*
[dependencies]

*/

use arroyo_udf_plugin::udf;

#[udf]
fn ${functionName}(x: i64) -> i64 {
    // Write your function here
    // Tip: rename the function to something descriptive
}`;

    const defaultPython = `
from arroyo_udf import udf
    
@udf
def ${functionName}(x: int) -> int:
    # Write your function here
    # Tip: rename the function to something descriptive`;

    let definition = language == 'python' ? defaultPython : defaultRust;

    const newUdf = { name: functionName, definition, id, language, open: true };
    const newLocalUdfs = [...localUdfs, newUdf];
    setLocalUdfs(newLocalUdfs);
    setSelectedUdfId(id);
  };

  return {
    localUdfs,
    setLocalUdfs,
    deleteUdf,
    deleteLocalUdf,
    deleteGlobalUdf,
    openedUdfs,
    openTab,
    closeUdf,
    isOverridden,
    updateLocalUdf,
    isGlobal,
    newUdf,
    editorTab,
    handleEditorTabChange,
  };
};

export const nameRoot = (name: string | undefined) => {
  return name?.split('/').pop() || 'unknown';
};
