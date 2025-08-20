import Box from "@mui/material/Box";
import Grid from "@mui/material/Grid";
import Input from "@mui/material/Input";
import Slider from "@mui/material/Slider";
import Typography from "@mui/material/Typography";

interface RangeSliderProps {
  min: number;
  max: number;
  setValue: any;
  value: number[];
  limit: number; // largest range allowed
}

const RangeSlider: React.FunctionComponent<RangeSliderProps> = (props) => {
  const handleSliderChange = (
    event: Event,
    newValue: number | number[],
    activeThumb: number,
  ) => {
    const range = newValue as number[];
    let safeValue = [0, 0];
    if (activeThumb === 0) {
      safeValue = [range[0], Math.min(range[1], range[0] + props.limit)];
    } else {
      safeValue = [Math.max(range[0], range[1] - props.limit), range[1]];
    }
    props.setValue(safeValue as number[]);
  };

  const handleMinInputChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    // If this change puts (max - min) > limit, change max to keep the result in bounds.
    props.setValue(
      event.target.value === ""
        ? props.value
        : [
            Number(event.target.value),
            Math.min(props.value[1], Number(event.target.value) + props.limit),
          ],
    );
  };

  const handleMaxInputChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    // If this change puts (max - min) > limit, change min to keep the result in bounds.
    props.setValue(
      event.target.value === ""
        ? props.value
        : [
            Math.max(props.value[0], Number(event.target.value) - props.limit),
            Number(event.target.value),
          ],
    );
  };

  const handleBlur = () => {
    if (props.value[0] < props.min) {
      props.setValue([props.min, props.value[1]]);
    } else if (props.value[1] > props.max) {
      props.setValue([props.value[0], props.max]);
    }
  };

  const marks = [
    { value: props.min, label: props.min },
    { value: props.max, label: props.max },
  ];

  return (
    <Box>
      <Typography id="input-slider" gutterBottom>
        {props.max - props.min <= props.limit
          ? "Optionally slice a range of elements from the array"
          : `Slice a range of up to ${props.limit} elements of the array`}
      </Typography>
      <Grid container spacing={2} alignItems="center">
        <Grid item xs>
          <Slider
            getAriaLabel={() => "Array slice range"}
            value={props.value}
            min={props.min}
            max={props.max}
            marks={marks}
            onChange={handleSliderChange}
            valueLabelDisplay="auto"
          />
        </Grid>
        <Grid item>
          <Input
            value={props.value[0]}
            size="small"
            onChange={handleMinInputChange}
            onBlur={handleBlur}
            inputProps={{
              step: 1,
              min: props.min,
              max: props.max,
              type: "number",
              "aria-labelledby": "min-input-slider",
            }}
          />
        </Grid>
        <Grid item>&ndash;</Grid>
        <Grid item>
          <Input
            value={props.value[1]}
            size="small"
            onChange={handleMaxInputChange}
            onBlur={handleBlur}
            inputProps={{
              step: 1,
              min: props.min,
              max: props.max,
              type: "number",
              "aria-labelledby": "max-input-slider",
            }}
          />
        </Grid>
      </Grid>
    </Box>
  );
};

export default RangeSlider;
