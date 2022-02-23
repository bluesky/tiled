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
import { ControlPointDuplicateRounded, Rowing } from '@mui/icons-material';

interface IProps {
  segments: string[]
  item: any
}

const DataFrameOverview: React.FunctionComponent<IProps> = (props) => {
  const [rows, setRows] = useState<any[]>([]);
  const [loadedRows, setLoadedRows] = useState<boolean>(false);
  useEffect(() => {
    const controller = new AbortController();
    async function loadRows() {
      var response = await axiosInstance.get(`${props.item.data.links.full}?format=application/json-seq`, {signal: controller.signal});
      const rows = response.data.split("\n").map((line: string) => JSON.parse(line)) as any[];
      setRows(rows);
      setLoadedRows(true);
    }
    loadRows();
    return () => { controller.abort() };
  }, [props.segments]);
  return (
    <Box sx={{ my: 4 }}>
      <Container maxWidth="lg">
        <DataDisplay rows={rows} columns={props.item.data.attributes.structure.macro.columns} loading={!loadedRows} />
      </Container>
    </Box>
  );
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
