import { useParams } from 'react-router-dom'
import { NodeOverview } from '../components/overview'
import { useState, useEffect } from 'react';
import { metadata } from '../client';
import { components } from '../openapi_schemas';

function Node() {
  // Extract from path from react-router.
  const params = useParams<{"*": string}>();
  // Transform "/a/b/c" to ["a", "b", "c"].
  const segments = (params["*"] || "").split("/").filter(function (segment) {return segment})
  const [item, setItem] = useState<components["schemas"]["Response_Resource_NodeAttributes__dict__dict___dict__dict_"]>();
  useEffect(() => {
    async function loadData() {
      var result = await metadata(segments);
      if (result !== undefined) {
        console.log(result);
        setItem(result);
      }
    }
    loadData();
  }, []);
  return (
    <div>
      <NodeOverview segments={segments} item={item} />
    </div>
  )
}

export default Node;
