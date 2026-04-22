import axios, { AxiosError, InternalAxiosRequestConfig } from "axios";
import { components } from "./openapi_schemas";
import {
  getStoredAccessToken,
  getStoredRefreshToken,
  storeTokens,
  clearTokens,
} from "./context/auth";

const axiosInstance = axios.create();

// Attach Bearer token to every request.
axiosInstance.interceptors.request.use((config: InternalAxiosRequestConfig) => {
  const token = getStoredAccessToken();
  if (token) {
    config.headers.set("Authorization", `Bearer ${token}`);
  }
  return config;
});

// On 401, attempt a token refresh; retry the original request on success.
let refreshPromise: Promise<boolean> | null = null;

axiosInstance.interceptors.response.use(
  (response) => response,
  async (error: AxiosError) => {
    const originalRequest = error.config as InternalAxiosRequestConfig & {
      _retry?: boolean;
    };
    if (
      error.response?.status === 401 &&
      originalRequest &&
      !originalRequest._retry
    ) {
      originalRequest._retry = true;
      const refreshToken = getStoredRefreshToken();
      if (refreshToken) {
        if (!refreshPromise) {
          refreshPromise = (async () => {
            try {
              const resp = await axios.post("/api/v1/auth/session/refresh", {
                refresh_token: refreshToken,
              });
              storeTokens(resp.data.access_token, resp.data.refresh_token);
              return true;
            } catch {
              clearTokens();
              return false;
            }
          })().finally(() => {
            refreshPromise = null;
          });
        }
        const success = await refreshPromise;
        if (success) {
          const token = getStoredAccessToken();
          if (token) {
            originalRequest.headers.set("Authorization", `Bearer ${token}`);
          }
          return axiosInstance(originalRequest);
        }
        // Had a session but refresh failed — reload to reach login page.
        window.location.reload();
      }
      // No refresh token — not logged in. Just let the 401 propagate.
    }
    return Promise.reject(error);
  },
);

export const search = async (
  apiURL: string,
  segments: string[],
  signal: AbortSignal,
  fields: string[] = [],
  selectMetadata: any = null,
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

export { axiosInstance };
