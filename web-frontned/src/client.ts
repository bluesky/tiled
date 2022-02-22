import axios from 'axios';
import { components } from './openapi_schemas';

declare global {
  interface Window { baseURL: string; }
}

let apiURL = process.env.REACT_APP_API_PREFIX
if (apiURL == null) {
  let apiURL = `$window.baseURL/api`;
}

var axiosInstance = axios.create({
  baseURL: apiURL,
});


export const search = async (
  segments: string[],
  signal: AbortSignal,
  fields: string[] = []
): Promise<string[]> => {
  const response = await axiosInstance.get(`/node/search/${segments.join('/')}?fields=${fields.join("&fields=")}`, {signal: signal});
  let ids: string[] = [];
  response.data.data.forEach((element: any) => {
    ids.push(element.id)
  });
  return ids;
}


export const metadata = async (
  segments: string[],
  signal: AbortSignal,
  fields: string[] = []
): Promise<components["schemas"]["Response_Resource_NodeAttributes__dict__dict___dict__dict_"]> => {
  const response = await axiosInstance.get(`/node/metadata/${segments.join('/')}?fields=${fields.join("&fields=")}`, {signal: signal});
  return response.data;
}

export { axiosInstance };
