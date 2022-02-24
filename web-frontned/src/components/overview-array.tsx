import * as React from "react";

import { useEffect, useState } from "react";

import { ArrayLineChart } from "./line";
import Container from "@mui/material/Container";
import FormControl from "@mui/material/FormControl";
import FormControlLabel from "@mui/material/FormControlLabel";
import FormLabel from "@mui/material/FormLabel";
import Radio from "@mui/material/Radio";
import RadioGroup from "@mui/material/RadioGroup";
import RangeSlider from "./range-slider";
import Skeleton from "@mui/material/Skeleton";
import { axiosInstance } from "../client";
import { debounce } from "ts-debounce";

interface DisplayRadioButtonsProps {
  value: string;
  handleChange: any;
}

const DisplayRadioButtons: React.FunctionComponent<DisplayRadioButtonsProps> = (
  props
) => {
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
};

interface DataDisplayProps {
  name: string;
  link: string;
  range: number[];
}

const DataDisplay: React.FunctionComponent<DataDisplayProps> = (props) => {
  const [displayType, setDisplayType] = useState<string>("chart");
  const [data, setData] = useState<any[]>([]);
  const [dataIsLoaded, setDataIsLoaded] = useState<boolean>(false);

  const handleDisplayTypeChange = (
    event: React.ChangeEvent<HTMLInputElement>
  ) => {
    setDisplayType((event.target as HTMLInputElement).value);
  };

  useEffect(() => {
    const controller = new AbortController();
    async function loadData() {
      var response = await axiosInstance.get(
        `${props.link}?format=application/json&slice=${props.range[0]}:${props.range[1]}`,
        { signal: controller.signal }
      );
      const data = response.data;
      setData(data);
      setDataIsLoaded(true);
    }
    loadData();
    return () => {
      controller.abort();
    };
  }, [props.link, props.range]);

  const display = () => {
    switch (displayType) {
      case "chart":
        return dataIsLoaded ? (
          <ArrayLineChart
            data={data}
            startingIndex={props.range[0]}
            name={props.name}
          />
        ) : (
          <Skeleton variant="rectangular" />
        );
      case "list":
        return dataIsLoaded ? (
          <ItemList data={data} />
        ) : (
          <Skeleton variant="rectangular" />
        );
    }
  };

  return (
    <div>
      <DisplayRadioButtons
        value={displayType}
        handleChange={handleDisplayTypeChange}
      />
      {display()}
    </div>
  );
};

interface ItemListProps {
  data: any[];
}

const ItemList: React.FunctionComponent<ItemListProps> = (props) => {
  return (
    <table>
      <tbody>
        {props.data.map((item, index) => {
          return (
            <tr key={`item-tr-${index}`}>
              <td key={`item-td-${index}`}>{item}</td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
};

interface IProps {
  segments: string[];
  item: any;
}

const LIMIT = 1000;  // largest number of elements we will request and display at once

const Array1D: React.FunctionComponent<IProps> = (props) => {
  const MAX_DEFAULT_RANGE = 100;
  const max = props.item.data.attributes.structure.macro.shape[0];
  const [value, setValue] = React.useState<number[]>([
    0,
    Math.min(max, MAX_DEFAULT_RANGE),
  ]);
  return (
    <div>
      <RangeSlider
        value={value}
        setValue={debounce(setValue, 100, { maxWait: 200 })}
        min={0}
        max={max}
        limit={LIMIT}
      />
      <DataDisplay
        link={props.item.data.links.full}
        range={value}
        name={props.item.data.id}
      />
    </div>
  );
};

const ArrayND: React.FunctionComponent<IProps> = (props) => {
  return (
    <img
      alt="Data rendered"
      src={props.item.data!.links!.full as string}
      loading="lazy"
    />
  );
};

const ArrayOverview: React.FunctionComponent<IProps> = (props) => {
  return (
    <Container maxWidth="lg">
      {props.item.data.attributes.structure.macro.shape.length < 2 ? (
        <Array1D segments={props.segments} item={props.item} />
      ) : (
        <ArrayND segments={props.segments} item={props.item} />
      )}
    </Container>
  );
};

export { ArrayOverview };
