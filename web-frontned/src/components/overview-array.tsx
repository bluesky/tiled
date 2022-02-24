import Container from '@mui/material/Container';
import Typography from '@mui/material/Typography';
import LoadingButton from '@mui/lab/LoadingButton';
import Box from '@mui/material/Box';
import Grid from '@mui/material/Grid';
import Stack from '@mui/material/Stack';
import JSONViewer from './json-viewer'
import { useState, useEffect } from 'react';
import { metadata } from '../client';
import { components } from '../openapi_schemas';
import * as React from 'react';
import Radio from '@mui/material/Radio';
import RadioGroup from '@mui/material/RadioGroup';
import FormControlLabel from '@mui/material/FormControlLabel';
import FormControl from '@mui/material/FormControl';
import FormLabel from '@mui/material/FormLabel';
import { axiosInstance } from '../client';
import Slider from '@mui/material/Slider';
import Input from '@mui/material/Input';
import Skeleton from '@mui/material/Skeleton';
import { ArrayLineChart } from './line';

function valuetext(value: number) {
  return `${value}°C`;
}

interface RangeSliderProps {
  min: number;
  max: number;
  setValue: any;
  value: number[];
}

const RangeSlider: React.FunctionComponent<RangeSliderProps> = (props) => {
  const handleSliderChange = (event: Event, newValue: number | number[]) => {
    props.setValue(newValue as number[]);
  };

  const handleMinInputChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    props.setValue(event.target.value === '' ? props.value : [Number(event.target.value), props.value[1]]);
  };

  const handleMaxInputChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    props.setValue(event.target.value === '' ? props.value : [props.value[0], Number(event.target.value)]);
  };

  const handleBlur = () => {
    if (props.value[0] < props.min) {
      props.setValue([props.min, props.value[1]]);
    } else if (props.value[1] > props.max) {
      props.setValue([props.value[0], props.max]);

    }
  };


  return (
    <Box sx={{ width: 500}}>
      <Typography id="input-slider" gutterBottom>
        Select slice of array
      </Typography>
      <Grid container spacing={2} alignItems="center">
        <Grid item xs>
          <Slider
            getAriaLabel={() => 'Array slice range'}
            value={props.value}
            min={props.min}
            max={props.max}
            disableSwap
            onChange={handleSliderChange}
            valueLabelDisplay="auto"
            getAriaValueText={valuetext}
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
              type: 'number',
              'aria-labelledby': 'min-input-slider',
            }}
          />
        </Grid>
        <Grid item>
          &ndash;
        </Grid>
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
              type: 'number',
              'aria-labelledby': 'max-input-slider',
            }}
          />
        </Grid>
      </Grid>
    </Box>
  );
}


interface DisplayRadioButtonsProps {
  value: string;
  handleChange: any;
}


const DisplayRadioButtons: React.FunctionComponent<DisplayRadioButtonsProps> = (props) => {
  return (
    <FormControl>
      <FormLabel id="display-radio-buttons-group-label">View as</FormLabel>
      <RadioGroup
        row
        aria-labelledby="display-radio-buttons-group-label"
        name="display-radio-buttons-group"
        value={props.value}
        onChange={props.handleChange}
      >
        <FormControlLabel value="chart" control={<Radio />} label="Chart" />
        <FormControlLabel value="list" control={<Radio />} label="List" />
      </RadioGroup>
    </FormControl>
  );
}

interface DataDisplayProps {
  link: string;
  range: number[];
}


const DataDisplay: React.FunctionComponent<DataDisplayProps> = (props) => {
  const [displayType, setDisplayType] = useState<string>("chart");
  const [data, setData] = useState<any[]>([]);
  const [dataIsLoaded, setDataIsLoaded] = useState<boolean>(false);

  const handleDisplayTypeChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setDisplayType((event.target as HTMLInputElement).value);
  };

  useEffect(() => {
    const controller = new AbortController();
    async function loadData() {
      var response = await axiosInstance.get(`${props.link}?format=application/json&slice=${props.range[0]}:${props.range[1]}`, {signal: controller.signal});
      const data = response.data;
      setData(data)
      setDataIsLoaded(true);
    }
    loadData();
    return () => { controller.abort() };
  }, [props.link, props.range]);

  const display = () => {
    switch(displayType) {
      case "chart": return dataIsLoaded ? <ArrayLineChart data={data} /> : <Skeleton variant="rectangular"/>
      case "list": return dataIsLoaded ? <ItemList data={data} /> : <Skeleton variant="rectangular"/>
    }
  }

  return (
    <div>
      <DisplayRadioButtons value={displayType} handleChange={handleDisplayTypeChange} />
      {display()}
    </div>
  )
}

interface ItemListProps {
  data: any[];
}


const ItemList: React.FunctionComponent<ItemListProps> = (props) => {
  return (
    <table>
      <tbody>
        {props.data.map((item, index) => { return <tr key={`item-tr-${index}`}><td key={`item-td-${index}`}>{item}</td></tr> })}
      </tbody>
    </table>
  )
}



interface IProps {
  segments: string[]
  item: any
}


const Array1D: React.FunctionComponent<IProps> = (props) => {
  const MAX_DEFAULT_RANGE = 100;
  const max = props.item.data.attributes.structure.macro.shape[0];
  const [value, setValue] = React.useState<number[]>([0, Math.min(max, MAX_DEFAULT_RANGE)]);
  return (
    <div>
      <RangeSlider value={value} setValue={setValue} min={0} max={max} />
      <DataDisplay link={props.item.data.links.full} range={value} />
    </div>
  )
}

const ArrayND: React.FunctionComponent<IProps> = (props) => {
  return <img alt="Data rendered" src={props.item.data!.links!.full as string} loading="lazy" />
}

const ArrayOverview: React.FunctionComponent<IProps> = (props) => {
  return (
    <Container maxWidth="lg">
      {props.item.data.attributes.structure.macro.shape.length < 2 ?  <Array1D segments={props.segments} item={props.item} /> : <ArrayND segments={props.segments} item={props.item} />}
    </Container>
  )
}

export { ArrayOverview };
