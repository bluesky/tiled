import * as React from "react";

import Box from "@mui/material/Box";
import Breadcrumbs from "@mui/material/Breadcrumbs";
import Link from "@mui/material/Link";
import { Link as RouterLink } from "react-router-dom";

interface IProps {
  segments: string[];
}

const NodeBreadcrumbs: React.FunctionComponent<IProps> = (props) => {
  if (props.segments !== undefined) {
    return (
      <Box mt={3} mb={2}>
        <Breadcrumbs aria-label="breadcrumb">
          <Link key="breadcrumb-0" component={RouterLink} to="/browse/">
            Top
          </Link>
          {props.segments.map((segment, index, segments) => (
            <Link
              component={RouterLink}
              key={"breadcrumb-{1 + i}" + segment}
              to={`/browse${segments
                .slice(0, 1 + index)
                .map((segment) => {
                  return "/" + segment;
                })
                .join("")}/`}
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
