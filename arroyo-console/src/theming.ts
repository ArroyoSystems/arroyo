import { modalAnatomy, popoverAnatomy } from '@chakra-ui/anatomy';
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
