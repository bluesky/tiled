import * as React from "react";

import Select, { SelectChangeEvent } from "@mui/material/Select";
import { useContext, useState } from "react";

import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import ContentCopyIcon from "@mui/icons-material/ContentCopy";
import FormControl from "@mui/material/FormControl";
import IconButton from "@mui/material/IconButton";
import InputLabel from "@mui/material/InputLabel";
import MenuItem from "@mui/material/MenuItem";
import Popover from "@mui/material/Popover";
import Stack from "@mui/material/Stack";
import TextField from "@mui/material/TextField";
import Tooltip from "@mui/material/Tooltip";
import { axiosInstance } from "../../client";
import copy from "clipboard-copy";
import { SettingsContext } from "../../context/settings";

interface Format {
  mimetype: string;
  display_name: string;
  extension: string;
}

interface DownloadProps {
  name: string;
  structureFamily: string;
  format: Format | undefined;
  setFormat: any;
  link: string;
}

const Download: React.FunctionComponent<DownloadProps> = (props) => {
  const settings = useContext(SettingsContext);
  const formats = settings.structure_families[props.structureFamily].formats;
  const [anchorEl, setAnchorEl] = React.useState<HTMLButtonElement | null>(
    null,
  );

  const handleLinkClick = (event: React.MouseEvent<HTMLButtonElement>) => {
    setAnchorEl(event.currentTarget);
  };

  const handleClose = () => {
    setAnchorEl(null);
  };

  const open = Boolean(anchorEl);
  const id = open ? "link-popover" : undefined;

  const handleChange = (event: SelectChangeEvent) => {
    const mimetype = event.target.value as string;
    const format = formats.find(
      (format: Format) => format.mimetype === mimetype,
    );
    props.setFormat(format);
  };

  const value = props.format !== undefined ? props.format.mimetype : "";

  const handleDownload = async () => {
    if (!props.link || !props.format) return;
    const url = `${props.link}&filename=${props.name}${props.format.extension}`;
    try {
      const resp = await axiosInstance.get(url, { responseType: "blob" });
      const blobUrl = URL.createObjectURL(resp.data);
      const a = document.createElement("a");
      a.href = blobUrl;
      a.download = `${props.name}${props.format.extension}`;
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(blobUrl);
    } catch (e) {
      console.error("Download failed", e);
    }
  };

  const handleOpen = async () => {
    if (!props.link) return;
    try {
      const resp = await axiosInstance.get(props.link, { responseType: "blob" });
      const blobUrl = URL.createObjectURL(resp.data);
      window.open(blobUrl, "_blank");
      // Revoke after a delay to allow the new tab to load
      setTimeout(() => URL.revokeObjectURL(blobUrl), 60000);
    } catch (e) {
      console.error("Open failed", e);
    }
  };

  return (
    <Stack spacing={2} direction="column">
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
                <MenuItem
                  key={`format-${format.mimetype}`}
                  value={format.mimetype}
                >
                  {format.display_name as string}
                </MenuItem>
              );
            })}
          </Select>
        </FormControl>
      </Box>
      <Stack spacing={1} direction="row">
        <Tooltip title="Download to a file">
          <span>
            <Button
              variant="outlined"
              onClick={handleDownload}
              {...(props.link ? {} : { disabled: true })}
            >
              Download
            </Button>
          </span>
        </Tooltip>
        <Tooltip title="Get a URL to this specific data">
          <span>
            <Button
              aria-describedby={id}
              variant="outlined"
              {...(props.link ? {} : { disabled: true })}
              onClick={handleLinkClick}
            >
              Link
            </Button>
          </span>
        </Tooltip>
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
        <Tooltip title="Open in a new tab (if format is supported by web browser)">
          <span>
            <Button
              variant="outlined"
              onClick={handleOpen}
              {...(props.link ? {} : { disabled: true })}
            >
              Open
            </Button>
          </span>
        </Tooltip>
      </Stack>
    </Stack>
  );
};

export { Download };
export type { Format };
