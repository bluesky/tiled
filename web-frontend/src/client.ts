import axios from "axios";
import { components } from "./openapi_schemas";

const axiosInstance = axios.create({
  headers: {
    "Content-Type": "application/json",
  },
});

export function setupAuthInterceptor(getAccessToken: () => string | null) {
  axiosInstance.interceptors.request.use(
    (config) => {
      const token = getAccessToken();
      if (token) {
        config.headers = config.headers || {};
        config.headers.Authorization = `Bearer ${token}`;
      }
      return config;
    },
    (error) => Promise.reject(error),
  );
}

export function setupRefreshInterceptor(
  getRefreshToken: () => string | null,
  refreshTokenFn: (refreshToken: string) => Promise<any>,
  saveTokens: (tokens: any) => void,
  clearTokens: () => void,
) {
  axiosInstance.interceptors.response.use(
    (response) => response,
    async (error) => {
      const originalRequest = error.config;

      if (error.response?.status === 401 && !originalRequest._retry) {
        originalRequest._retry = true;

        try {
          const refreshToken = getRefreshToken();

          if (!refreshToken) {
            throw new Error("No refresh token available");
          }

          const newTokens = await refreshTokenFn(refreshToken);
          saveTokens(newTokens);

          originalRequest.headers = originalRequest.headers || {};
          originalRequest.headers.Authorization = `Bearer ${newTokens.access_token}`;
          return axiosInstance(originalRequest);
        } catch (refreshError) {
          console.error(refreshError);
          clearTokens();

          // Redirect to login
          if (typeof window !== "undefined") {
            window.location.href = "/ui/login";
          }

          return Promise.reject(refreshError);
        }
      }

      return Promise.reject(error);
    },
  );
}

export const search = async (
  apiURL: string,
  segments: string[],
  signal: AbortSignal,
  fields: string[] = [],
  selectMetadata: string | null = null,
  pageOffset: number = 0,
  pageLimit: number = 100,
): Promise<any> => {
  const fieldsParam =
    fields.length > 0 ? `&fields=${fields.join("&fields=")}` : "";
  let url = `${apiURL}/search/${segments.join("/")}?page[offset]=${pageOffset}&page[limit]=${pageLimit}${fieldsParam}`;

  if (selectMetadata !== null) {
    url += `&select_metadata=${selectMetadata}`;
  }

  const response = await axiosInstance.get(url, { signal });
  return response.data;
};

export const metadata = async (
  apiURL: string,
  segments: string[],
  signal: AbortSignal,
  fields: string[] = [],
): Promise<any> => {
  const fieldsParam =
    fields.length > 0 ? `?fields=${fields.join("&fields=")}` : "";
  const url = `${apiURL}/metadata/${segments.join("/")}${fieldsParam}`;

  const response = await axiosInstance.get(url, { signal });
  return response.data;
};

export const about = async (apiURL: string = "/api/v1"): Promise<any> => {
  const response = await axiosInstance.get(`${apiURL}/`);
  return response.data;
};

export { axiosInstance };
