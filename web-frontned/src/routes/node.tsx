import { useParams } from 'react-router-dom'
import { ArrayOverview, NodeOverview } from '../components/overview'
import { useState, useEffect } from 'react';
import { metadata } from '../client';
import { components } from '../openapi_schemas';
import NodeBreadcrumbs from '../components/node-breadcrumbs';
import Box from '@mui/material/Box';
import Skeleton from '@mui/material/Skeleton';

interface IProps {
  segments: string[],
}

const Overview: React.FunctionComponent<IProps> = (props) => {
  // Dispatch to a specific overview component based on the structure family.
  // In the future we will extend this to consider 'specs' as well.
  const [item, setItem] = useState<components["schemas"]["Response_Resource_NodeAttributes__dict__dict___dict__dict_"]>();
  useEffect(() => {
    async function loadData() {
      // Request only enough information to decide which React component
      // we should use to display this. Let the component request
      // more detailed information.
      var result = await metadata(props.segments, ["structure_family", "specs"]);
      if (result !== undefined) {
        setItem(result);
      }
    }
    loadData();
  }, [props.segments]);
  if (item !== undefined) {
    const structureFamily = item!.data!.attributes!.structure_family
    switch(structureFamily) {
      case "node": return <NodeOverview segments={props.segments} item={item} />
      case "array": return <ArrayOverview segments={props.segments} item={item} />
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
          <Overview segments={segments} />
        </Box>
      </div>
    )
  } else { return <Skeleton variant="text" />}
}

export default Node;
