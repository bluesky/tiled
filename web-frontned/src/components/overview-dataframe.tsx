import Container from '@mui/material/Container';
import Typography from '@mui/material/Typography';
import LoadingButton from '@mui/lab/LoadingButton';
import Box from '@mui/material/Box';
import Stack from '@mui/material/Stack';
import JSONViewer from './json-viewer'
import { useState, useEffect } from 'react';
import { axiosInstance, metadata } from '../client';
import { DataGrid, GridRowParams } from '@mui/x-data-grid';
import { components } from '../openapi_schemas';
import { Rowing } from '@mui/icons-material';

interface IProps {
  segments: string[]
  item: any
}

const DataFrameOverview: React.FunctionComponent<IProps> = (props) => {
  const [fullItem, setFullItem] = useState<components["schemas"]["Response_Resource_NodeAttributes__dict__dict___dict__dict_"]>();
  const [rows, setRows] = useState<any[]>([]);
  const [loadedRows, setLoadedRows] = useState<boolean>(false);
  useEffect(() => {
    async function loadFullItem() {
      // Request all the attributes.
      var result = await metadata(props.segments, ["structure_family", "structure.macro", "structure.micro", "specs", "metadata", "sorting", "count"]);
      if (result !== undefined) {
        setFullItem(result);
      }
    }
    loadFullItem();
  }, [props.segments]);
  useEffect(() => {
    async function loadRows() {
      var response = await axiosInstance.get(`${props.item.data.links.full}?format=application/json-seq`);
      const rows = response.data.split("\n").map((line: string) => JSON.parse(line)) as any[];
      setRows(rows);
      setLoadedRows(true);
    }
    loadRows();
  }, [props.segments]);
  if (props.item && props.item.data) {
    return (
      <Box sx={{ my: 4 }}>
        <Container maxWidth="lg">
          <Typography variant="h4" component="h1" gutterBottom>
            {props.item.data.id || "Top"}
          </Typography>
          <Stack direction="row" spacing={2}>
            { fullItem ? <JSONViewer json={fullItem} /> : <LoadingButton loading loadingIndicator="Loading...">Loading...</LoadingButton>}
          </Stack>
          <DataDisplay rows={rows} columns={props.item.data.attributes.structure.macro.columns} loading={!loadedRows} />
        </Container>
      </Box>
    );
  }
  return <div>Loading...</div>
}

interface IDataDisplayProps {
  columns: string[]
  rows: any[]
  loading: boolean
}

const DataDisplay: React.FunctionComponent<IDataDisplayProps> = (props) => {
  const data_columns = props.columns.map((column) => ({ field: column, headerName: column, width: 200 }));
  const data_rows = props.rows.map((row, index) => { row.id = index; return row });
  return (
    <Box sx={{ my: 4 }}>
      <Container maxWidth="lg">
        <DataGrid
          {...(props.loading ? {"loading": true} : {})}
          rows={data_rows}
          columns={data_columns}
          pageSize={30}
          rowsPerPageOptions={[10, 30, 100]}
          autoHeight
        />
      </Container>
    </Box>
  );
}

export { DataFrameOverview };
