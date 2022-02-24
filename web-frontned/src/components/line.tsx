import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import Box from '@mui/material/Box';

interface ArrayLineChartProps {
  data: number[];
}


const ArrayLineChart: React.FunctionComponent<ArrayLineChartProps> = (props) => {
  console.log(props.data.map((value, index) => { return {"index": index, "x": value}}));
  return (
    <Box height={300}>
    <ResponsiveContainer>
    <LineChart
      width={500}
      height={300}
      data={props.data.map((value, index) => { return {"index": index, "x": value}})}
      margin={{
        top: 5,
        right: 30,
        left: 20,
        bottom: 5,
      }}
    >
      <CartesianGrid strokeDasharray="3 3" />
      <XAxis dataKey="index" />
      <YAxis />
      <Tooltip />
      <Legend />
      <Line type="monotone" stroke="#000000" dataKey="x" />
    </LineChart>
    </ResponsiveContainer>
    </Box>
  );
}

export { ArrayLineChart };
