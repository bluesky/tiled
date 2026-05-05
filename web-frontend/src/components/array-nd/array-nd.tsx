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

/**
 * Returns true if the last dimension of the shape looks like a color channel
 * (RGB = 3, RGBA = 4). This lets us treat (H, W, 3) and (H, W, 4) arrays as
 * color images rather than stacks of 2-D planes.
 */
function isColorImage(shape: number[]): boolean {
  return shape.length >= 3 && (shape[shape.length - 1] === 3 || shape[shape.length - 1] === 4);
}

const ArrayND: React.FunctionComponent<IProps> = (props) => {
  const shape = props.structure!.shape as number[];
  const ndim = shape.length;

  // Determine how many trailing dimensions belong to the image plane.
  // For color images (last dim == 3 or 4) the image plane is (H, W, C) → 3 dims.
  // For grayscale the image plane is (H, W) → 2 dims.
  const imageDims = isColorImage(shape) ? 3 : 2;

  // The number of "stack" dimensions that get sliders.
  const stackDims = ndim - imageDims;

  // Find the middle slice index for each stack dimension.
  const middles = shape
    .slice(0, stackDims)
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

  // Compute a downsampling stride based on the largest spatial dimension
  // (H and W, i.e. the two dimensions just before the optional color channel).
  const spatialDims = shape.slice(stackDims, stackDims + 2);
  const stride = Math.ceil(Math.max(...spatialDims) / maxImageSize);

  const [cuts, setCuts] = useState<number[]>(middles);
  return (
    <Box>
      <ImageDisplay
        link={props.link}
        cuts={cuts}
        stride={stride}
        isColor={isColorImage(shape)}
      />
      {stackDims > 0 ? (
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
      {shape.slice(0, stackDims).map((size: number, index: number) => {
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
  isColor: boolean;
}

const ImageDisplay: React.FunctionComponent<ImageDisplayProps> = (props) => {
  const [blobUrl, setBlobUrl] = useState<string | null>(null);

  // Build the slice string:
  //   - One integer per stack dimension (the "cut" value)
  //   - ::stride for H (height)
  //   - ::stride for W (width)
  //   - For color images, no striding on the color channel (just ":" to select all)
  const sliceParts = props.cuts.map(c => c.toString());
  if (props.stride !== 1) {
    sliceParts.push(`::${props.stride}`, `::${props.stride}`);
    if (props.isColor) {
      sliceParts.push(`:`);
    }
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
