import React, { useContext, useMemo, useState } from 'react';
import { CodeEditor } from '../pipelines/CodeEditor';
import { LocalUdf, LocalUdfsContext } from '../../udf_state';
import { GlobalUdf, UdfValidationResult, useUdfValidation } from '../../lib/data_fetching';
import { debounce } from 'lodash';

export interface UdfEditorProps {
  udf: LocalUdf | GlobalUdf;
}

const UdfEditor: React.FC<UdfEditorProps> = ({ udf }) => {
  const { updateLocalUdf, isGlobal } = useContext(LocalUdfsContext);
  const [definitionToCheck, setDefinitionToCheck] = useState<string>(udf.definition);

  const debounceSetCheck = useMemo(
    () =>
      debounce((s: string) => {
        setDefinitionToCheck(s);
      }, 500),
    []
  );

  const updateName = (data: UdfValidationResult, key: any, config: any) => {
    if (data.udfName) {
      updateLocalUdf(udf as LocalUdf, { name: data.udfName });
    }
  };

  useUdfValidation(updateName, definitionToCheck);

  return (
    <CodeEditor
      code={udf.definition}
      readOnly={isGlobal(udf)}
      setCode={s => {
        updateLocalUdf(udf as LocalUdf, { definition: s });
        debounceSetCheck(s);
      }}
      language="rust"
    />
  );
};

export default UdfEditor;
