import * as React from "react";

import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Checkbox from "@mui/material/Checkbox";
import CutSlider from "../cut-slider/cut-slider";
import FormControl from "@mui/material/FormControl";
import FormControlLabel from "@mui/material/FormControlLabel";
import InputLabel from "@mui/material/InputLabel";
import MenuItem from "@mui/material/MenuItem";
import Select, { SelectChangeEvent } from "@mui/material/Select";
import Stack from "@mui/material/Stack";
import Typography from "@mui/material/Typography";
import { COLORMAP_LABELS, COLORMAPS, ColormapName } from "./colormaps";
import { components } from "../../openapi_schemas";
import { debounce } from "ts-debounce";
import useMediaQuery from "@mui/material/useMediaQuery";
import { useEffect, useRef, useState, useCallback } from "react";
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
  return (
    shape.length >= 3 &&
    (shape[shape.length - 1] === 3 || shape[shape.length - 1] === 4)
  );
}

const ArrayND: React.FunctionComponent<IProps> = (props) => {
  const shape = props.structure!.shape as number[];
  const ndim = shape.length;
  const color = isColorImage(shape);

  // Determine how many trailing dimensions belong to the image plane.
  // For color images (last dim == 3 or 4) the image plane is (H, W, C) → 3 dims.
  // For grayscale the image plane is (H, W) → 2 dims.
  const imageDims = color ? 3 : 2;

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
  const [colormap, setColormap] = useState<ColormapName>("gray");
  const [logScale, setLogScale] = useState<boolean>(false);

  const onFirstLoad = useCallback(
    (suggested: { logScale: boolean; colormap: ColormapName }) => {
      setLogScale(suggested.logScale);
      setColormap(suggested.colormap);
    },
    [],
  );

  return (
    <Box>
      {color ? (
        <PngImageDisplay
          link={props.link}
          cuts={cuts}
          stride={stride}
          isColor={true}
        />
      ) : (
        <GrayscaleImageDisplay
          link={props.link}
          cuts={cuts}
          stride={stride}
          structure={props.structure}
          colormap={colormap}
          logScale={logScale}
          onFirstLoad={onFirstLoad}
        />
      )}

      {/* Colormap + log-scale controls — only for grayscale */}
      {!color && (
        <Stack direction="row" spacing={2} alignItems="center" sx={{ mt: 1 }}>
          <FormControl size="small" sx={{ minWidth: 130 }}>
            <InputLabel id="colormap-label">Colormap</InputLabel>
            <Select
              labelId="colormap-label"
              value={colormap}
              label="Colormap"
              onChange={(e: SelectChangeEvent) =>
                setColormap(e.target.value as ColormapName)
              }
            >
              {COLORMAP_LABELS.map(({ value, label }) => (
                <MenuItem key={value} value={value}>
                  {label}
                </MenuItem>
              ))}
            </Select>
          </FormControl>
          <FormControlLabel
            control={
              <Checkbox
                checked={logScale}
                onChange={(e) => setLogScale(e.target.checked)}
                size="small"
              />
            }
            label="Log scale"
          />
        </Stack>
      )}

      {shape.slice(0, stackDims).some((size) => size > 1) ? (
        <Typography id="input-slider" gutterBottom sx={{ mt: 1 }}>
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

// ---------------------------------------------------------------------------
// Shared image CSS — fills container width, caps height so sliders stay visible
// ---------------------------------------------------------------------------
const imageSx = {
  width: "100%",
  maxHeight: "60vh",
  objectFit: "contain" as const,
  display: "block",
};

// ---------------------------------------------------------------------------
// PNG path — used for color (RGB/RGBA) images served by the backend directly
// ---------------------------------------------------------------------------
interface PngImageDisplayProps {
  link: string;
  cuts: number[];
  stride: number;
  isColor: boolean;
}

const PngImageDisplay: React.FunctionComponent<PngImageDisplayProps> = (
  props,
) => {
  const [blobUrl, setBlobUrl] = useState<string | null>(null);

  // Build the slice string:
  //   - One integer per stack dimension (the "cut" value)
  //   - ::stride for H (height) and W (width)
  //   - For color images, no striding on the color channel (just ":" to select all)
  const sliceParts = props.cuts.map((c) => c.toString());
  if (props.stride !== 1) {
    sliceParts.push(`::${props.stride}`, `::${props.stride}`);
    if (props.isColor) {
      sliceParts.push(`:`);
    }
  }
  const url =
    sliceParts.length > 0
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
    <Box component="img" sx={imageSx} alt="Data rendered" src={blobUrl} />
  );
};

// ---------------------------------------------------------------------------
// Grayscale canvas path — fetches raw bytes, applies colormap + log scaling
// ---------------------------------------------------------------------------
interface GrayscaleImageDisplayProps {
  link: string;
  cuts: number[];
  stride: number;
  structure: components["schemas"]["Structure"];
  colormap: ColormapName;
  logScale: boolean;
  onFirstLoad?: (settings: { logScale: boolean; colormap: ColormapName }) => void;
}

/**
 * Suggest initial display settings from the pixel distribution of a
 * freshly-fetched grayscale slice.
 *
 * Detector/scientific images with sparse bright pixels have a huge gap
 * between the 99th percentile and the maximum value — most photons land
 * on a small number of pixels, leaving the rest near zero. We detect
 * this with:
 *
 *   max / (p99 + ε) > 10  →  log scale + Viridis
 *   otherwise             →  linear + Gray
 *
 * This correctly handles:
 *   - Photographs / microscopy with smooth histograms (ratio ~1)
 *   - Binary/boolean images (ratio ~1)
 *   - Detector images with outlier bright pixels (ratio >> 10)
 */
function suggestSettings(raw: ArrayLike<number>): { logScale: boolean; colormap: ColormapName } {
  const n = raw.length;
  if (n === 0) return { logScale: false, colormap: "gray" };

  const values: number[] = [];
  for (let i = 0; i < n; i++) {
    const v = raw[i];
    if (isFinite(v)) values.push(v);
  }
  if (values.length === 0) return { logScale: false, colormap: "gray" };
  values.sort((a, b) => a - b);

  const lo = values[0];
  const hi = values[values.length - 1];
  const p99 = values[Math.floor(0.99 * (values.length - 1))];

  // Shift by minimum to handle data with a large DC offset or negative values
  const hiS  = hi  - lo;
  const p99S = p99 - lo;

  const logScale = hiS > 10 * (p99S + 1e-6);
  const colormap: ColormapName = logScale ? "viridis" : "gray";
  return { logScale, colormap };
}

/**
 * Map a numpy dtype descriptor (kind + itemsize) to a JS TypedArray constructor
 * and the byte-swap flag needed when the server sends little-endian data.
 */
function dtypeToTypedArray(
  kind: string,
  itemsize: number,
): {
  ArrayType: new (buffer: ArrayBuffer) => ArrayLike<number>;
  needsByteSwap: boolean;
} {
  // The server always sends native (little-endian on x86) byte order via
  // application/octet-stream.  JS typed arrays use the platform's native
  // order (also little-endian on x86), so no swap is needed in practice.
  // We still carry the flag for correctness.
  const needsByteSwap = false;
  switch (`${kind}${itemsize}`) {
    case "b1": return { ArrayType: Uint8Array, needsByteSwap };
    case "u1": return { ArrayType: Uint8Array, needsByteSwap };
    case "u2": return { ArrayType: Uint16Array, needsByteSwap };
    case "u4": return { ArrayType: Uint32Array, needsByteSwap };
    case "i1": return { ArrayType: Int8Array, needsByteSwap };
    case "i2": return { ArrayType: Int16Array, needsByteSwap };
    case "i4": return { ArrayType: Int32Array, needsByteSwap };
    case "f4": return { ArrayType: Float32Array, needsByteSwap };
    case "f8": return { ArrayType: Float64Array, needsByteSwap };
    default:   return { ArrayType: Float32Array, needsByteSwap };
  }
}

const GrayscaleImageDisplay: React.FunctionComponent<
  GrayscaleImageDisplayProps
> = (props) => {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const { link, cuts, stride, structure, colormap, logScale, onFirstLoad } = props;
  // Fire onFirstLoad only once per mount (not on colormap/logScale changes).
  const firstLoadFired = useRef(false);

  const dataType = (structure as any).data_type as {
    kind: string;
    itemsize: number;
  };

  // Build the slice URL (same logic as PngImageDisplay, but no color suffix
  // and requesting raw binary)
  const sliceParts = cuts.map((c) => c.toString());
  if (stride !== 1) {
    sliceParts.push(`::${stride}`, `::${stride}`);
  }
  const url =
    sliceParts.length > 0
      ? `${link}?format=application/octet-stream&slice=${sliceParts.join(",")}`
      : `${link}?format=application/octet-stream`;

  useEffect(() => {
    const controller = new AbortController();
    axiosInstance
      .get(url, { responseType: "arraybuffer", signal: controller.signal })
      .then((resp) => {
        const canvas = canvasRef.current;
        if (!canvas) return;

        const { ArrayType } = dtypeToTypedArray(
          dataType?.kind ?? "f",
          dataType?.itemsize ?? 4,
        );
        const raw = new ArrayType(resp.data as ArrayBuffer) as ArrayLike<number>;
        const n = raw.length;

        // On first load, suggest colormap/logScale from the pixel distribution
        // and propagate to the parent. Only fires once per mount so user
        // changes to the controls are not overridden on subsequent fetches.
        if (!firstLoadFired.current && onFirstLoad) {
          firstLoadFired.current = true;
          onFirstLoad(suggestSettings(raw));
        }

        // --- Normalise to [0, 255] ----------------------------------------
        // Find finite min/max (skip NaN/Inf for float arrays)
        let lo = Infinity;
        let hi = -Infinity;
        for (let i = 0; i < n; i++) {
          const v = raw[i];
          if (isFinite(v)) {
            if (v < lo) lo = v;
            if (v > hi) hi = v;
          }
        }
        if (lo === hi) hi = lo + 1; // avoid divide-by-zero for constant arrays

        const lut = COLORMAPS[colormap];

        // Determine canvas dimensions from the shape after slicing:
        // shape after cuts = [H_strided, W_strided] (stride already applied by server)
        const shape = structure!.shape as number[];
        const ndim = shape.length;
        const stackDims = ndim - 2; // grayscale: last 2 dims are spatial
        const H = Math.ceil(shape[stackDims] / stride);
        const W = Math.ceil(shape[stackDims + 1] / stride);
        canvas.width = W;
        canvas.height = H;

        const ctx = canvas.getContext("2d");
        if (!ctx) return;
        const imgData = ctx.createImageData(W, H);
        const px = imgData.data; // Uint8ClampedArray, 4 bytes per pixel (RGBA)

        for (let i = 0; i < n; i++) {
          let v = raw[i];
          if (logScale) {
            // Shift to positive before log: log1p(v - lo) / log1p(hi - lo)
            v = Math.log1p(v - lo) / Math.log1p(hi - lo);
          } else {
            v = (v - lo) / (hi - lo);
          }
          // Clamp and map to [0, 255]
          const idx = Math.min(255, Math.max(0, Math.round(v * 255)));
          const [r, g, b] = lut[idx];
          const p = i * 4;
          px[p] = r;
          px[p + 1] = g;
          px[p + 2] = b;
          px[p + 3] = 255; // fully opaque
        }
        ctx.putImageData(imgData, 0, 0);
      })
      .catch(() => {});
    return () => controller.abort();
  }, [url, colormap, logScale, dataType?.kind, dataType?.itemsize, stride, structure]);

  return (
    <Box
      component="canvas"
      ref={canvasRef}
      sx={imageSx}
      aria-label="Data rendered"
    />
  );
};

export default ArrayND;
