import { useState, useEffect } from 'react';
import { Link, useParams } from 'react-router-dom';
import Container from '@mui/material/Container';
import Box from '@mui/material/Box';
import { search } from '../client';


interface IProps {
  segments: string[]
}

const Contents: React.FunctionComponent<IProps> = (props) => {
  const [items, setItems] = useState<string[]>([]);

  useEffect(() => {
    async function loadData() {
      var results = await search(props.segments);
      if (results !== undefined) {
        setItems(results);
      }
    }
    loadData();
  }, [props.segments]);
  return (
    <Box sx={{ my: 4 }}>
      <Container maxWidth="sm">
        <ul>
        {items.map(id=> (
          <li key={"li-" + id}>
            <Link key={"link-" + id} to={"/node" + props.segments.map(function (segment) {return "/" + segment}) + "/" + id}>{id}</Link>
          </li>
        ))
        }
        </ul>
      </Container>
    </Box>
  );
}

export default Contents;
