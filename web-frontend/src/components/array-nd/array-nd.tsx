import * as React from "react";

import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import CutSlider from "../cut-slider/cut-slider";
import Typography from "@mui/material/Typography";
import { components } from "../../openapi_schemas";
import { debounce } from "ts-debounce";
import useMediaQuery from "@mui/material/useMediaQuery";
import { useEffect, useState } from "react";
import { useTheme } from "@mui/material/styles";
import { axiosInstance } from "../../client";

interface IProps {
  segments: string[];
  link: string;
  item: any;
  structure: components["schemas"]["Structure"];
}

const ArrayND: React.FunctionComponent<IProps> = (props) => {
  const shape = props.structure!.shape as number[];
  const ndim = shape.length;
  // Find the middle slice in all dimensions except the last two.
  const middles = shape
    .slice(0, ndim - 2)
    .map((size: number) => Math.floor(size / 2));
  // Request an image from the server that is downsampled to be at most
  // 2X as big as it will be displayed.
  const theme = useTheme();
  const sm = useMediaQuery(theme.breakpoints.down("sm"));
  const md = useMediaQuery(theme.breakpoints.down("md"));
  let maxImageSize: number;
  if (sm) {
    maxImageSize = theme.breakpoints.values.sm;
  } else if (md) {
    maxImageSize = theme.breakpoints.values.md;
  } else {
    maxImageSize = theme.breakpoints.values.lg;
  }
  // Compute a downsampling stride based on the largest dimension
  // among the last two.
  const stride = Math.ceil(
    Math.max(...shape.slice(ndim - 2, ndim)) / maxImageSize,
  );
  const [cuts, setCuts] = useState<number[]>(middles);
  return (
    <Box>
      <ImageDisplay link={props.link} cuts={cuts} stride={stride} />
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
      {shape.slice(0, ndim - 2).map((size: number, index: number) => {
        if (size > 1) {
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
                { maxWait: 200 },
              )}
            />
          );
        } else {
          // Dimension of length 1, nothing to select.
          return "";
        }
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
  const [blobUrl, setBlobUrl] = useState<string | null>(null);
  const sliceParts = props.cuts.map(c => c.toString());
  if (props.stride !== 1) {
    sliceParts.push(`::${props.stride}`, `::${props.stride}`);
  }
  const url = sliceParts.length > 0
    ? `${props.link}?format=image/png&slice=${sliceParts.join(",")}`
    : `${props.link}?format=image/png`;

  useEffect(() => {
    let objectUrl: string | null = null;
    const controller = new AbortController();
    axiosInstance
      .get(url, { responseType: "blob", signal: controller.signal })
      .then((resp) => {
        objectUrl = URL.createObjectURL(resp.data);
        setBlobUrl(objectUrl);
      })
      .catch(() => {});
    return () => {
      controller.abort();
      if (objectUrl) URL.revokeObjectURL(objectUrl);
    };
  }, [url]);

  if (!blobUrl) return null;
  return (
    <Box
      component="img"
      sx={{ maxWidth: 1 }}
      alt="Data rendered"
      src={blobUrl}
    />
  );
};

export default ArrayND;
