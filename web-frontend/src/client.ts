import axios from "axios";
import { components } from "./openapi_schemas";

export const axiosInstance = axios.create();

// Transform absolute URLs in "links" fields to relative paths so the UI
// works regardless of the origin the server reports.
function toRelativePath(urlString: string): string {
  try {
    const url = new URL(urlString);
    return url.pathname + url.search + url.hash;
  } catch {
    return urlString;
  }
}

function transformLinks(data: any): any {
  if (typeof data === "object" && data !== null) {
    const transformed: any = Array.isArray(data) ? [] : {};
    for (const key in data) {
      if (key === "links" && typeof data[key] === "object") {
        transformed[key] = {};
        for (const linkKey in data[key]) {
          const linkValue = data[key][linkKey];
          transformed[key][linkKey] =
            typeof linkValue === "string"
              ? toRelativePath(linkValue)
              : linkValue;
        }
      } else {
        transformed[key] = transformLinks(data[key]);
      }
    }
    return transformed;
  }
  return data;
}

axiosInstance.interceptors.response.use(
  (response) => {
    if (response.config.responseType === "blob") {
      return response;
    }
    response.data = transformLinks(response.data);
    return response;
  },
  (error) => Promise.reject(error),
);

export const search = async (
  apiURL: string,
  segments: string[],
  signal: AbortSignal,
  fields: string[] = [],
  selectMetadata: string | null = null,
  pageOffset: number = 0,
  pageLimit: number = 100,
  sort: string | null = null,
): Promise<
  components["schemas"]["Response_List_tiled.server.router.Resource_NodeAttributes__dict__dict____PaginationLinks__dict_"]
> => {
  let url = `${apiURL}/search/${segments.join(
    "/",
  )}?page[offset]=${pageOffset}&page[limit]=${pageLimit}&fields=${fields.join(
    "&fields=",
  )}`;
  if (selectMetadata !== null) {
    url = url.concat(`&select_metadata=${selectMetadata}`);
  }
  if (sort) {
    url = url.concat(`&sort=${encodeURIComponent(sort)}`);
  }
  const response = await axiosInstance.get(url, { signal: signal });
  return response.data;
};

export const metadata = async (
  apiURL: string,
  segments: string[],
  signal: AbortSignal,
  fields: string[] = [],
): Promise<
  components["schemas"]["Response_Resource_NodeAttributes__dict__dict___dict__dict_"]
> => {
  const response = await axiosInstance.get(
    `${apiURL}/metadata/${segments.join("/")}?fields=${fields.join(
      "&fields=",
    )}`,
    { signal: signal },
  );
  return response.data;
};

export const about = async (): Promise<components["schemas"]["About"]> => {
  const response = await axiosInstance.get("/api/v1/");
  return response.data;
};
