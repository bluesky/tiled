import axios from "axios";
import { components } from "./openapi_schemas";

const apiURL = process.env.REACT_APP_API_PREFIX || "../api"

var axiosInstance = axios.create({
  baseURL: apiURL,
});

export const search = async (
  segments: string[],
  signal: AbortSignal,
  fields: string[] = []
): Promise<components["schemas"]["Resource_NodeAttributes__dict__dict_"][]> => {
  const response = await axiosInstance.get(
    `/node/search/${segments.join("/")}?fields=${fields.join("&fields=")}`,
    { signal: signal }
  );
  return response.data.data;
};

export const metadata = async (
  segments: string[],
  signal: AbortSignal,
  fields: string[] = []
): Promise<
  components["schemas"]["Response_Resource_NodeAttributes__dict__dict___dict__dict_"]
> => {
  const response = await axiosInstance.get(
    `/node/metadata/${segments.join("/")}?fields=${fields.join("&fields=")}`,
    { signal: signal }
  );
  return response.data;
};

export { axiosInstance };
