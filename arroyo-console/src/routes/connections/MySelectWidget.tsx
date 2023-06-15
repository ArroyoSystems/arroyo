import { FocusEvent } from 'react';
import { FormControl, FormLabel, Select } from '@chakra-ui/react';
import {
  ariaDescribedByIds,
  EnumOptionsType,
  enumOptionsIndexForValue,
  enumOptionsValueForIndex,
  labelValue,
  FormContextType,
  RJSFSchema,
  StrictRJSFSchema,
  WidgetProps,
} from '@rjsf/utils';

export default function MySelectWidget<
  T = any,
  S extends StrictRJSFSchema = RJSFSchema,
  F extends FormContextType = any
>(props: WidgetProps<T, S, F>) {
  const {
    id,
    options,
    label,
    hideLabel,
    placeholder,
    multiple,
    required,
    disabled,
    readonly,
    value,
    autofocus,
    onChange,
    onBlur,
    onFocus,
    rawErrors = [],
    uiSchema,
  } = props;
  const { enumOptions, enumDisabled, emptyValue } = options;

  const _onChange = (e: any) => {
    console.log(e.value);
    return onChange(enumOptionsValueForIndex<S>(e.value, enumOptions, emptyValue));
  };

  const _onBlur = ({ target: { value } }: FocusEvent<HTMLInputElement>) =>
    onBlur(id, enumOptionsValueForIndex<S>(value, enumOptions, emptyValue));

  const _onFocus = ({ target: { value } }: FocusEvent<HTMLInputElement>) =>
    onFocus(id, enumOptionsValueForIndex<S>(value, enumOptions, emptyValue));

  const _valueLabelMap: any = {};
  const displayEnumOptions = Array.isArray(enumOptions)
    ? enumOptions.map((option: EnumOptionsType<S>, index: number) => {
        const { value, label } = option;
        _valueLabelMap[index] = label || String(value);
        return {
          label,
          value: String(index),
          isDisabled: Array.isArray(enumDisabled) && enumDisabled.indexOf(value) !== -1,
        };
      })
    : [];

  const isMultiple = typeof multiple !== 'undefined' && Boolean(enumOptions);
  const selectedIndex = enumOptionsIndexForValue<S>(value, enumOptions, isMultiple);
  const formValue: any = isMultiple
    ? ((selectedIndex as string[]) || []).map((i: string) => {
        return {
          label: _valueLabelMap[i],
          value: i,
        };
      })
    : {
        label: _valueLabelMap[selectedIndex as string] || '',
        selectedIndex,
      };

  return (
    <FormControl
      mb={1}
      isDisabled={disabled || readonly}
      isRequired={required}
      isReadOnly={readonly}
      isInvalid={rawErrors && rawErrors.length > 0}
    >
      {labelValue(
        <FormLabel htmlFor={id} id={`${id}-label`}>
          {label}
        </FormLabel>,
        hideLabel || !label
      )}
      <Select
        inputId={id}
        name={id}
        placeholder={placeholder}
        closeMenuOnSelect={true}
        onBlur={_onBlur}
        onChange={_onChange}
        onFocus={_onFocus}
        autoFocus={autofocus}
        aria-describedby={ariaDescribedByIds<T>(id)}
      >
        { displayEnumOptions.map(({ label, value, isDisabled }) => (
            <option key={value} value={value} disabled={isDisabled}>
                {label}
            </option>
        ))}
    </Select>
    </FormControl>
  );
}
