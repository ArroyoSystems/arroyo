import './index.css';

import { extendTheme, ThemeConfig } from '@chakra-ui/react';
import { theme as proTheme } from '@chakra-ui/pro-theme';
import '@fontsource/inter/variable.css';

import { createRoot } from './lib/CloudComponents';
import { modalTheme, popoverTheme } from './theming';

const config: ThemeConfig = {
  initialColorMode: 'dark',
  useSystemColorMode: false,
};

const theme = extendTheme(proTheme, {
  colors: { ...proTheme.colors, brand: proTheme.colors.blue },
  config: config,
  components: { Modal: modalTheme, Popover: popoverTheme },
});

const rootElement = document.getElementById('root');
createRoot(rootElement!, theme);
