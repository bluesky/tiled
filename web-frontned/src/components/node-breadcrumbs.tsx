import * as React from 'react';
import Breadcrumbs from '@mui/material/Breadcrumbs';
import Link from '@mui/material/Link';

interface IProps {
  segments: string[]
}

const NodeBreadcrumbs: React.FunctionComponent<IProps> = (props) => {
  if (props.segments !== undefined) {
    return (
      <Breadcrumbs aria-label="breadcrumb">
        <Link key="breadcrumb-0" href="/node/">Top</Link>
        {props.segments.map((segment, index, segments) => (
          <Link
            key={"breadcrumb-{1 + i}" + segment}
            href={"/node" + segments.slice(0, 1 + index).map((segment) => {return "/" + segment}) + "/"}
          >
            {segment}
          </Link>
        ))
        }
      </Breadcrumbs>
    );
  }
  else { return <div>...</div> }
}

export default NodeBreadcrumbs;
