import Container from '@mui/material/Container';
import Typography from '@mui/material/Typography';
import LoadingButton from '@mui/lab/LoadingButton';
import Box from '@mui/material/Box';
import Stack from '@mui/material/Stack';
import JSONViewer from './json-viewer'
import { useState, useEffect } from 'react';
import { metadata } from '../client';
import { components } from '../openapi_schemas';

interface IProps {
  segments: string[]
  item: any
}

const ArrayOverview: React.FunctionComponent<IProps> = (props) => {
  return (
    <Container maxWidth="lg">
      <img alt="Data rendered" src={props.item.data!.links!.full as string} loading="lazy" />
    </Container>
  )
}

export { ArrayOverview };
