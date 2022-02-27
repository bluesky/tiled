import * as React from "react";

import Select, { SelectChangeEvent } from "@mui/material/Select";
import { about, axiosInstance } from "../client";
import { useEffect, useMemo, useState } from "react";

import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import ContentCopyIcon from "@mui/icons-material/ContentCopy";
import FormControl from "@mui/material/FormControl";
import IconButton from "@mui/material/IconButton";
import InputLabel from "@mui/material/InputLabel";
import MenuItem from "@mui/material/MenuItem";
import Popover from "@mui/material/Popover";
import Skeleton from "@mui/material/Skeleton";
import Stack from "@mui/material/Stack";
import TextField from "@mui/material/TextField";
import Tooltip from "@mui/material/Tooltip";
import { components } from "../openapi_schemas";
import copy from "clipboard-copy";

interface Format {
  mimetype: string;
  displayName: string;
  extension: string;
}

interface DownloadProps {
  name: string;
  structure_family: string;
  format: Format | undefined;
  setFormat: any;
  link: string;
}

const Download: React.FunctionComponent<DownloadProps> = (props) => {
  const [info, setInfo] = useState<components["schemas"]["About"]>();
  const [anchorEl, setAnchorEl] = React.useState<HTMLButtonElement | null>(
    null
  );

  const handleLinkClick = (event: React.MouseEvent<HTMLButtonElement>) => {
    setAnchorEl(event.currentTarget);
  };

  const handleClose = () => {
    setAnchorEl(null);
  };

  const formats = JSON.parse(sessionStorage.getItem("config") as string).formats

  const open = Boolean(anchorEl);
  const id = open ? "link-popover" : undefined;


  useMemo(() => {
    async function loadInfo() {
      var result = await about();
      setInfo(result);
    }
    loadInfo();
  }, []);

  const handleChange = (event: SelectChangeEvent) => {
    const mimetype = event.target.value as string;
    const format = formats.find((format: Format) => format.mimetype === mimetype);
    props.setFormat(format);
  };

  if (info === undefined) {
    return <Skeleton variant="rectangular" />;
  }
  const value = (props.format !== undefined) ? props.format.mimetype : "";

  return (
    <Stack spacing={2} direction="row">
      <Box sx={{ minWidth: 120 }}>
        <FormControl fullWidth>
          <InputLabel id="formats-select-label">Format *</InputLabel>
          <Select
            labelId="formats-select-label"
            id="formats-select"
            value={value}
            label="Format"
            onChange={handleChange}
            required
          >
            {formats.map((format: Format) => {
              return (
                // Look up the display name in the UI configuration.
                // If none is given, skip this format.
                info!.formats[props.structure_family].includes(format.mimetype) ? <MenuItem key={`format-${format.mimetype}`} value={format.mimetype}>{format.displayName as string}</MenuItem> : ""
              );
            })}
          </Select>
        </FormControl>
      </Box>
      {
        // The filename query parameter cues the server to set the
        // Content-Disposition header which prompts the browser to open
        // a "Save As" dialog initialized with the specified filename.
      }
      <Button
        component="a"
        href={props.format ? `${props.link}&filename=${props.name}${props.format!.extension}` : "#"}
        variant="outlined"
        {...(props.format ? {} : { disabled: true })}
      >
        Download
      </Button>
      <Button
        aria-describedby={id}
        variant="outlined"
        {...(props.format ? {} : { disabled: true })}
        onClick={handleLinkClick}
      >
        Link
      </Button>
      <Popover
        id={id}
        open={open}
        anchorEl={anchorEl}
        onClose={handleClose}
        anchorOrigin={{
          vertical: "bottom",
          horizontal: "right",
        }}
        transformOrigin={{
          vertical: "top",
          horizontal: "right",
        }}
        PaperProps={{
          style: { width: 500 },
        }}
      >
        <Box sx={{ px: 2, py: 2 }}>
          <Stack direction="row" spacing={1} />
          <TextField
            id="link-text"
            label="Link"
            sx={{ width: "90%" }}
            defaultValue={props.link}
            InputProps={{
              readOnly: true,
            }}
            variant="outlined"
          />
          <Tooltip title="Copy to clipboard">
            <IconButton
              onClick={() => {
                copy(props.link);
              }}
            >
              <ContentCopyIcon />
            </IconButton>
          </Tooltip>
        </Box>
      </Popover>
    </Stack>
  );
};

export {Download};
export type {Format};
