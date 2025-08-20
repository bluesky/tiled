import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import Box from "@mui/material/Box";

interface ArrayLineChartProps {
  data: number[];
  startingIndex: number;
  name: string;
}

const ArrayLineChart: React.FunctionComponent<ArrayLineChartProps> = (
  props,
) => {
  return (
    <Box height={300}>
      <ResponsiveContainer>
        <LineChart
          width={500}
          height={300}
          data={props.data.map((value, index) => {
            return { index: index + props.startingIndex, [props.name]: value };
          })}
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
          <Line type="monotone" stroke="#000000" dataKey={props.name} />
        </LineChart>
      </ResponsiveContainer>
    </Box>
  );
};

export { ArrayLineChart };
