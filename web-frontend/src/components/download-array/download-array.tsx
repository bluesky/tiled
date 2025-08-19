import * as React from "react";

import { Download, Format } from "../download-core/download-core";

import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Container from "@mui/material/Container";
import Popover from "@mui/material/Popover";
import Stack from "@mui/material/Stack";
import TextField from "@mui/material/TextField";
import { useState } from "react";

interface DownloadArrayProps {
  name: string;
  structureFamily: string;
  structure: any;
  specs: string[];
  link: string;
}

function Examples() {
  const [anchorEl, setAnchorEl] = React.useState<HTMLButtonElement | null>(
    null,
  );

  const handleClick = (event: React.MouseEvent<HTMLButtonElement>) => {
    setAnchorEl(event.currentTarget);
  };

  const handleClose = () => {
    setAnchorEl(null);
  };

  const open = Boolean(anchorEl);
  const id = open ? "examples" : undefined;

  return (
    <div>
      <Button aria-describedby={id} variant="text" onClick={handleClick}>
        Examples
      </Button>
      <Popover
        id={id}
        open={open}
        anchorEl={anchorEl}
        onClose={handleClose}
        anchorOrigin={{
          vertical: "bottom",
          horizontal: "left",
        }}
      >
        <Box sx={{ px: 2, py: 2 }}>
          This supports basic multi-dimensional{" "}
          <strong>numpy array slicing syntax</strong>. Examples for a
          2-dimensional array:
          <ul>
            <li>
              First 50 elements along the first axis:
              <pre>
                <code>:50</code>
              </pre>
            </li>
            <li>
              First 50 elements along the second axis:
              <pre>
                <code>:,:50</code>
              </pre>
            </li>
            <li>
              A 50x10 section from the bottom right
              <pre>
                <code>-50:,-10:</code>
              </pre>
            </li>
            <li>
              Downsample the first and second axis by 2:{" "}
              <pre>
                <code>::2,::2</code>
              </pre>
            </li>
          </ul>
          Also see this{" "}
          <a
            href="https://www.w3schools.com/python/numpy/numpy_array_slicing.asp"
            target="_blank"
            rel="noreferrer"
          >
            beginner tutorial
          </a>
          .
        </Box>
      </Popover>
    </div>
  );
}

const DownloadArray: React.FunctionComponent<DownloadArrayProps> = (props) => {
  const [format, setFormat] = useState<Format>();
  const [slice, setSlice] = useState<string>("");
  var link: string;
  if (format !== undefined) {
    link = `${props.link}?format=${format.mimetype}`;
    if (slice) {
      link = link.concat(`&slice=${slice}`);
    }
  } else {
    link = "";
  }

  const handleInputChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    // Strip any spaces.
    // The server rejects them as invalid characters, but we will
    // tolerate them as input for readability.
    setSlice(event.target.value.replace(/ /g, ""));
  };

  return (
    <Box>
      <Stack spacing={2} direction="column">
        <Container>Dimensions: {props.structure.shape.join(" × ")}</Container>
        <Stack spacing={1} direction="row">
          <TextField
            label="Slice (Optional)"
            helperText="If blank, access entire array"
            variant="outlined"
            value={slice}
            size="medium"
            onChange={handleInputChange}
          />
          <Examples />
        </Stack>
        <Download
          name={props.name}
          format={format}
          setFormat={setFormat}
          structureFamily={props.structureFamily}
          link={link}
        />
        {format !== undefined &&
        (format.mimetype.startsWith("image/") ||
          format.mimetype.startsWith("text/")) &&
        props.structure.shape.length !== 2 ? (
          <Alert sx={{ mt: 2 }} severity="warning">
            This is a multidimensional array. It may be necessary to slice a
            portion of this array to successfully export it as an image or
            textual format.
          </Alert>
        ) : (
          ""
        )}
      </Stack>
    </Box>
  );
};

export default DownloadArray;
