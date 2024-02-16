import './index.css';

import { extendTheme, ThemeConfig } from '@chakra-ui/react';
import { theme as proTheme } from '@chakra-ui/pro-theme';
import '@fontsource/inter/variable.css';

import { modalTheme, popoverTheme, tabsTheme } from './theming';
import { createRoot } from './lib/CloudComponents';

const config: ThemeConfig = {
  initialColorMode: 'dark',
  useSystemColorMode: false,
};

const theme = extendTheme(proTheme, {
  colors: { ...proTheme.colors, brand: proTheme.colors.blue },
  config: config,
  components: { Modal: modalTheme, Popover: popoverTheme, Tabs: tabsTheme },
});

const rootElement = document.getElementById('root');
createRoot(rootElement!, theme);
