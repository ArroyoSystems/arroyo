import { modalAnatomy, popoverAnatomy, tabsAnatomy } from '@chakra-ui/anatomy';
import { createMultiStyleConfigHelpers } from '@chakra-ui/styled-system';

const { definePartsStyle: modalPartsStyle, defineMultiStyleConfig: defineModalMultiStyleConfig } =
  createMultiStyleConfigHelpers(modalAnatomy.keys);

export const modalTheme = defineModalMultiStyleConfig({
  variants: {
    tour: modalPartsStyle({
      dialog: {
        bg: `gray.100`,
        color: `black`,
      },
    }),
  },
});

const {
  definePartsStyle: popoverPartsStyle,
  defineMultiStyleConfig: definePopoverMultiStyleConfig,
} = createMultiStyleConfigHelpers(popoverAnatomy.keys);

export const popoverTheme = definePopoverMultiStyleConfig({
  baseStyle: popoverPartsStyle({
    content: {
      overflow: 'unset',
    },
    arrow: {
      '--popper-arrow-bg': 'var(--chakra-colors-gray-800)',
    },
  }),
  variants: {
    tour: popoverPartsStyle({
      content: {
        color: 'black',
        borderRadius: '4px',
        bg: `gray.100`,
        overflow: 'unset',
      },
      header: {
        bg: `gray.100`,
        borderBottomWidth: 0,
        fontWeight: `bold`,
        paddingBottom: 0,
      },
      body: {
        bg: `gray.100`,
      },
      arrow: {
        '--popper-arrow-bg': 'var(--chakra-colors-gray-100)',
      },
    }),
  },
});

const {
  definePartsStyle: defineTabsPartsStyle,
  defineMultiStyleConfig: defineTabsMultiStyleConfig,
} = createMultiStyleConfigHelpers(tabsAnatomy.keys);

// export the component theme
export const tabsTheme = defineTabsMultiStyleConfig({
  variants: {
    colorful: defineTabsPartsStyle({
      tab: {
        bg: 'gray.200',
        borderRadius: 10,
        _selected: {
          bg: 'gray.600',
          color: 'green',
        },
      },
    }),
  },
});
