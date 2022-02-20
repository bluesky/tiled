import { Link } from "react-router-dom";
import AppBar from '@mui/material/AppBar';
import Toolbar from '@mui/material/Toolbar';
import Typography from '@mui/material/Typography';
import Container from '@mui/material/Container';

const TiledAppBar = () => {
  return (
    <AppBar position="static">
      <Container maxWidth="xl">
        <Toolbar disableGutters>
          <Typography
            variant="h6"
            noWrap
            component="div"
            sx={{ mr: 2, display: { xs: 'none', md: 'flex' } }}
          >
            <Link to="/node/" color="inherit">Browse</Link>
            <Link to="/apikeys/" color="inherit">Manage API Keys</Link>
          </Typography>

        </Toolbar>
      </Container>
    </AppBar>
  );
};
export default TiledAppBar;
