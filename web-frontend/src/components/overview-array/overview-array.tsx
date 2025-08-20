import * as React from "react";

import { Suspense, lazy } from "react";

import Alert from "@mui/material/Alert";
import Skeleton from "@mui/material/Skeleton";
import { components } from "../../openapi_schemas";

const Array1D = lazy(() => import("../array-1d/array-1d"));
const ArrayND = lazy(() => import("../array-nd/array-nd"));

interface IProps {
  segments: string[];
  item: any;
  link: string;
  structure: components["schemas"]["Structure"];
}

const ArrayOverview: React.FunctionComponent<IProps> = (props) => {
  if (props.structure!.data_type!.hasOwnProperty("fields")) {
    return (
      <Alert severity="warning">
        This is a "record array" with a{" "}
        <a
          href="https://numpy.org/doc/stable/user/basics.rec.html"
          target="_blank"
          rel="noreferrer"
        >
          structured data type
        </a>
        . The web interface cannot view it. Use the "Download" tab to access the
        data.
      </Alert>
    );
  }
  const shape = props.structure!.shape as number[];
  switch (shape.length < 2) {
    case true:
      return (
        <Suspense fallback={<Skeleton variant="rectangular" />}>
          <Array1D
            segments={props.segments}
            item={props.item}
            link={props.link}
            structure={props.structure}
          />
        </Suspense>
      );
    case false:
      return (
        <Suspense fallback={<Skeleton variant="rectangular" />}>
          <ArrayND
            segments={props.segments}
            item={props.item}
            link={props.link}
            structure={props.structure}
          />
        </Suspense>
      );
  }
};

export default ArrayOverview;
