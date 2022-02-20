import { useParams } from 'react-router-dom'
import { NodeOverview } from '../components/overview'
import { useState, useEffect } from 'react';
import { metadata } from '../client';
import { components } from '../openapi_schemas';
import NodeBreadcrumbs from '../components/node-breadcrumbs';
import Box from '@mui/material/Box';

function nodeForStructureFamily(segments: string[], item: components["schemas"]["Response_Resource_NodeAttributes__dict__dict___dict__dict_"]) {
  const structureFamily = item!.data!.attributes!.structure_family
  console.log("structure family", structureFamily);
  switch(structureFamily) {
    case "node": return <NodeOverview segments={segments} item={item} />
    default: return <div>Unknown structure family {structureFamily}</div>
  }
}

function Node() {
  // Extract from path from react-router.
  const params = useParams<{"*": string}>();
  // Transform "/a/b/c" to ["a", "b", "c"].
  const segments = (params["*"] || "").split("/").filter(function (segment) {return segment})
  console.log(segments);
  const [item, setItem] = useState<components["schemas"]["Response_Resource_NodeAttributes__dict__dict___dict__dict_"]>();
  useEffect(() => {
    async function loadData() {
      // Request structure information but not user metadata, which may be large.
      var result = await metadata(segments, ["structure_family", "structure", "specs"]);
      if (result !== undefined) {
        setItem(result);
      }
    }
    loadData();
  }, [params]);


  if (item !== undefined) {
    console.log(JSON.stringify(item));
    return (
      <div>
        <Box sx={{mt: 3, mb: 3}}>
          <NodeBreadcrumbs segments={segments} />
          {nodeForStructureFamily(segments, item!)}
        </Box>
      </div>
    )
  } else { return <div>Loading...</div>}
}

export default Node;
