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

interface DownloadProps {
  structure_family: string;
  format: string;
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

  const open = Boolean(anchorEl);
  const id = open ? "link-popover" : undefined;

  const download = () => {
    const controller = new AbortController();
    async function loadData() {
      const response = await axiosInstance.get(`${props.link}`, {
        signal: controller.signal,
      });
      // This is sad and will not scale. We need a better way.
      // https://medium.com/@drevets/you-cant-prompt-a-file-download-with-the-content-disposition-header-using-axios-xhr-sorry-56577aa706d6
      const url = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement("a");
      link.href = url;
      link.setAttribute("download", "file");
      document.body.appendChild(link);
      link.click();
    }
    loadData();
    return () => {
      controller.abort();
    };
  };

  useMemo(() => {
    async function loadInfo() {
      var result = await about();
      setInfo(result);
    }
    loadInfo();
  }, []);

  const handleChange = (event: SelectChangeEvent) => {
    props.setFormat(event.target.value as string);
  };

  const handleDownloadClick = (event: React.MouseEvent) => {
    download();
  };

  if (info === undefined) {
    return <Skeleton variant="rectangular" />;
  }

  return (
    <Stack spacing={2} direction="row">
      <Box sx={{ minWidth: 400, width: { flex: 0.7 } }}>
        <FormControl fullWidth>
          <InputLabel id="formats-select-label">Format *</InputLabel>
          <Select
            labelId="formats-select-label"
            id="formats-select"
            value={props.format}
            label="Format"
            onChange={handleChange}
            required
          >
            {info!.formats[props.structure_family].map((format) => {
              return (
                <MenuItem key={`format-${format}`} value={format}>
                  {format}
                </MenuItem>
              );
            })}
          </Select>
        </FormControl>
      </Box>

      <Button
        variant="outlined"
        {...(props.format ? {} : { disabled: true })}
        onClick={handleDownloadClick}
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

export default Download;
