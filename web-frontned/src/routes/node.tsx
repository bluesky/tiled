import { useParams } from 'react-router-dom'
import Contents from '../components/contents'
import Metadata from '../components/metadata'

function Node() {
  const params = useParams<{"*": string}>();
  const segments = (params["*"] || "").split("/").filter(function (segment) {return segment})
  return (
    <div>
      <Metadata segments={segments} />
      <Contents segments={segments} />
    </div>
  )
}

export default Node;
