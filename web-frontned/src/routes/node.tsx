import { useParams } from 'react-router-dom'
import { NodeOverview } from '../components/overview-generic-node'
import { ArrayOverview } from '../components/overview-array'
import { DataFrameOverview } from '../components/overview-dataframe'
import { useState, useEffect } from 'react';
import { metadata } from '../client';
import { components } from '../openapi_schemas';
import NodeBreadcrumbs from '../components/node-breadcrumbs';
import Box from '@mui/material/Box';
import Skeleton from '@mui/material/Skeleton';

interface IProps {
  segments: string[],
}

const OverviewDispatch: React.FunctionComponent<IProps> = (props) => {
  // Dispatch to a specific overview component based on the structure family.
  // In the future we will extend this to consider 'specs' as well.
  const [item, setItem] = useState<components["schemas"]["Response_Resource_NodeAttributes__dict__dict___dict__dict_"]>();
  useEffect(() => {
    const controller = new AbortController();
    async function loadData() {
      // Request only enough information to decide which React component
      // we should use to display this. Let the component request
      // more detailed information.
      var result = await metadata(props.segments, controller.signal, ["structure_family", "specs", "structure.macro"]);
      if (result !== undefined) {
        setItem(result);
      }
    }
    loadData();
    return () => { controller.abort(); }
  }, [props.segments]);
  if (item !== undefined) {
    const structureFamily = item!.data!.attributes!.structure_family
    switch(structureFamily) {
      case "node": return <NodeOverview segments={props.segments} item={item} />
      case "array": return <ArrayOverview segments={props.segments} item={item} />
      case "dataframe": return <DataFrameOverview segments={props.segments} item={item} />
      default: return <div>Unknown structure family "{structureFamily}"</div>
    }
  }
    return <Skeleton variant="rectangular" />
}

function Node() {
  // Extract from path from react-router.
  const params = useParams<{"*": string}>();
  // Transform "/a/b/c" to ["a", "b", "c"].
  const segments = (params["*"] || "").split("/").filter(function (segment) {return segment})

  if (segments !== undefined) {
    return (
      <div>
        <Box sx={{mt: 3, mb: 3}}>
          <NodeBreadcrumbs segments={segments} />
          <OverviewDispatch segments={segments} />
        </Box>
      </div>
    )
  } else { return <Skeleton variant="text" />}
}

export default Node;
