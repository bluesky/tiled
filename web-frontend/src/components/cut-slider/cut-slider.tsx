import Box from "@mui/material/Box";
import Grid from "@mui/material/Grid";
import Input from "@mui/material/Input";
import Slider from "@mui/material/Slider";

interface CutSliderProps {
  min: number;
  max: number;
  setValue: any;
  value: number;
}

const CutSlider: React.FunctionComponent<CutSliderProps> = (props) => {
  const handleSliderChange = (event: Event, newValue: number | number[]) => {
    props.setValue(newValue);
  };

  const handleInputChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    props.setValue(
      event.target.value === "" ? props.value : Number(event.target.value),
    );
  };

  const handleBlur = () => {
    if (props.value < props.min) {
      props.setValue(props.min);
    } else if (props.value > props.max) {
      props.setValue(props.max);
    }
  };

  const marks = [
    { value: props.min, label: props.min },
    { value: props.max, label: props.max },
  ];

  return (
    <Box>
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
            value={props.value}
            size="small"
            onChange={handleInputChange}
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
      </Grid>
    </Box>
  );
};

export default CutSlider;
