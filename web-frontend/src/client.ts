import axios from "axios";
import { components } from "./openapi_schemas";

const axiosInstance = axios.create({
  //baseURL: "/api/v1",
  headers: {
    "Content-Type": "application/json",
  },
});

// Helper to convert absolute URLs to relative paths
function toRelativePath(urlString: string): string {
  try {
    const url = new URL(urlString);
    return url.pathname + url.search + url.hash;
  } catch {
    return urlString;
  }
}

// Transform all links in the response to relative paths
function transformLinks(data: any): any {
  if (!data) return data;

  if (typeof data === "string" && data.startsWith("http")) {
    return toRelativePath(data);
  }

  if (Array.isArray(data)) {
    return data.map(transformLinks);
  }

  if (typeof data === "object") {
    const transformed: any = {};
    for (const key in data) {
      if (key === "links" && typeof data[key] === "object") {
        // Transform all link values
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

// Add response interceptor to transform all links
axiosInstance.interceptors.response.use(
  (response) => {
    response.data = transformLinks(response.data);
    return response;
  },
  (error) => Promise.reject(error),
);

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
