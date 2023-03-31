import { Textarea } from "@chakra-ui/react";
import Editor from "@monaco-editor/react";
import { Dispatch } from "react";

export function SqlEditor({ query, setQuery, readOnly }: { query: string; setQuery?: Dispatch<string>, readOnly?: boolean }) {
  const onChange = (value: string | undefined) => {
    if (setQuery != null) {
      setQuery(value || "");
    }
  };

  return (
    <Editor
      height="50vh"
      defaultLanguage="sql"
      onChange={onChange}
      theme="vs-dark"
      options={{ minimap: { enabled: false }, wordWrap: "on", readOnly: readOnly || false }}
      value={query}
    />
  );
}
