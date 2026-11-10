import * as React from "react";

import Breadcrumbs from "@mui/material/Breadcrumbs";
import Link from "@mui/material/Link";
import { Link as RouterLink } from "react-router-dom";

interface IProps {
  segments: string[];
}

const NodeBreadcrumbs: React.FunctionComponent<IProps> = (props) => {
  if (props.segments !== undefined) {
    return (
      <Breadcrumbs
        aria-label="breadcrumb"
        maxItems={7}
        itemsAfterCollapse={3}
        itemsBeforeCollapse={1}
      >
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
    );
  } else {
    return <div>...</div>;
  }
};

export default NodeBreadcrumbs;
