import * as React from "react";

import { useEffect, useState } from "react";

import Alert from "@mui/material/Alert";
import { ArrayLineChart } from "./line";
import Box from "@mui/material/Box";
import { ConstructionOutlined } from "@mui/icons-material";
import CutSlider from "./cut-slider";
import FormControl from "@mui/material/FormControl";
import FormControlLabel from "@mui/material/FormControlLabel";
import FormLabel from "@mui/material/FormLabel";
import Radio from "@mui/material/Radio";
import RadioGroup from "@mui/material/RadioGroup";
import RangeSlider from "./range-slider";
import Skeleton from "@mui/material/Skeleton";
import Typography from "@mui/material/Typography";
import { axiosInstance } from "../client";
import { components } from "../openapi_schemas";
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
  structure: components["schemas"]["Structure"];
}

const LIMIT = 1000; // largest number of 1D elements we will request and display at once
const MAX_SIZE = 800; // max image size

const Array1D: React.FunctionComponent<IProps> = (props) => {
  const MAX_DEFAULT_RANGE = 100;
  const shape = props.structure!.macro!.shape! as number[];
  const max = shape[0];
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
  const shape = props.structure!.macro!.shape as number[];
  const middles = shape.slice(2).map((size: number) => Math.floor(size / 2));
  const stride = Math.ceil(Math.max(...shape.slice(0, 2)) / MAX_SIZE);
  const [cuts, setCuts] = useState<number[]>(middles);
  return (
    <Box>
      <ImageDisplay
        link={props.item.data!.links!.full as string}
        cuts={cuts}
        stride={stride}
      />
      {shape.length > 2 ? (
        <Typography id="input-slider" gutterBottom>
          Choose a planar cut through this {shape.length}-dimensional array.
        </Typography>
      ) : (
        ""
      )}
      {stride !== 1 ? (
        <Alert severity="info">
          This large array has been downsampled by a factor of {stride}.
          <br />
          Use the "Download" tab to access a full-resolution image.
        </Alert>
      ) : (
        ""
      )}
      {shape.slice(2).map((size: number, index: number) => {
        return (
          <CutSlider
            key={`slider-${index}`}
            min={0}
            max={size - 1}
            value={cuts[index]}
            setValue={debounce(
              (value) => {
                const newCuts = cuts.slice();
                newCuts[index] = value;
                setCuts(newCuts);
              },
              100,
              { maxWait: 200 }
            )}
          />
        );
      })}
    </Box>
  );
};

interface ImageDisplayProps {
  link: string;
  cuts: number[];
  stride: number;
}

const ImageDisplay: React.FunctionComponent<ImageDisplayProps> = (props) => {
  return (
    <Box
      component="img"
      alt="Data rendered"
      src={`${props.link}?format=image/png&slice=${props.cuts.join(",")},::${
        props.stride
      },::${props.stride}`}
      loading="lazy"
    />
  );
};

const ArrayOverview: React.FunctionComponent<IProps> = (props) => {
  if (props.structure!.micro!.fields) {
    return (
      <Alert severity="warning">
        This is a "record array" with a{" "}
        <a
          href="https://numpy.org/doc/stable/user/basics.rec.html"
          target="_blank"
        >
          structured data type
        </a>
        . The web interface cannot view it. Use the "Download" tab to access the
        data.
      </Alert>
    );
  }
  const shape = props.structure!.macro!.shape as number[];
  switch (shape.length < 2) {
    case true:
      return <Array1D segments={props.segments} item={props.item} structure={props.structure} />;
    case false:
      return <ArrayND segments={props.segments} item={props.item} structure={props.structure} />;
  }
};

export { ArrayOverview };
