import { useParams } from 'react-router-dom'
import Contents from '../components/contents'
import Metadata from '../components/metadata'

function Node() {
  // Extract from path from react-router.
  const params = useParams<{"*": string}>();
  // Transform "/a/b/c" to ["a", "b", "c"].
  const segments = (params["*"] || "").split("/").filter(function (segment) {return segment})
  return (
    <div>
      <Metadata segments={segments} />
      <Contents segments={segments} />
    </div>
  )
}

export default Node;
