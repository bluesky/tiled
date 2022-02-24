import * as React from "react";

import Box from "@mui/material/Box";
import Breadcrumbs from "@mui/material/Breadcrumbs";
import Link from "@mui/material/Link";

interface IProps {
  segments: string[];
}

const NodeBreadcrumbs: React.FunctionComponent<IProps> = (props) => {
  if (props.segments !== undefined) {
    return (
      <Box mt={3} mb={2}>
        <Breadcrumbs aria-label="breadcrumb">
          <Link key="breadcrumb-0" href="/node/">
            Top
          </Link>
          {props.segments.map((segment, index, segments) => (
            <Link
              key={"breadcrumb-{1 + i}" + segment}
              href={
                "/node" +
                segments.slice(0, 1 + index).map((segment) => {
                  return "/" + segment;
                }) +
                "/"
              }
            >
              {segment}
            </Link>
          ))}
        </Breadcrumbs>
      </Box>
    );
  } else {
    return <div>...</div>;
  }
};

export default NodeBreadcrumbs;
