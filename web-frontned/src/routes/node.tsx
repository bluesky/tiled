import { Outlet } from 'react-router-dom'
import Contents from '../components/contents'
import Metadata from '../components/metadata'

function NodePage() {
  return (
    <div>
      <Metadata />
      <Contents />
      <Outlet />
    </div>
  )
}

export default NodePage;
