import { Column, Spec } from "../contents/contents";
import { useContext, useEffect, useState } from "react";

import Container from "@mui/material/Container";
import NodeLazyContents from "../node-lazy-contents/node-lazy-contents";
import { Skeleton } from "@mui/material";
import Typography from "@mui/material/Typography";
import { SettingsContext } from "../../context/settings";

interface IProps {
  segments: string[];
  item: any;
}

const NodeOverview: React.FunctionComponent<IProps> = (props) => {
  const settings = useContext(SettingsContext);
  const specs = settings.specs || [];
  // Walk through the node's specs until we find one we recognize.
  const spec = specs.find((spec: Spec) =>
    props.item.data!.attributes!.specs.includes(spec.spec),
  );
  let columns: Column[];
  let defaultColumns: string[];

  // if (spec === undefined || !spec.columns || spec.columns.length === 0) {
  //   columns = [{ header: "ID", field: "id", select_metadata: "" }];
  //   defaultColumns = ["id"];
  // } else {
  //   columns = spec.columns;
  //   defaultColumns = spec.default_columns;

  if (spec === undefined) {
    columns = [];
    defaultColumns = ["id"];
  } else {
    columns = spec.columns;
    defaultColumns = spec.default_columns;
  }
  console.log(
    "spec:",
    spec,
    "columns:",
    columns,
    "specs:",
    specs,
    "item specs:",
    props.item.data!.attributes!.specs,
  );
  return (
    <Container maxWidth="lg">
      <Typography id="table-title" variant="h6" component="h2">
        Contents
      </Typography>
      {props.item.data!.attributes.structure_family === "container" && (
        <NodeLazyContents
          segments={props.segments}
          specs={props.item.data!.attributes!.specs!}
          columns={columns}
          defaultColumns={defaultColumns}
          structureFamily={props.item.data!.attributes!.structure_family}
        />
      )}
    </Container>
  );
};

export default NodeOverview;
