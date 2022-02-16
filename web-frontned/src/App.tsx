import { Outlet, Link } from "react-router-dom";
import React from 'react';

function App() {
  return (
  <div>
    <nav>
      <Link to="/node">Browse</Link>
      <Link to="/apikeys">Manage API Keys</Link>
    </nav>
    <Outlet />
  </div>
  );
}

export default App;
