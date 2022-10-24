import axios, {AxiosRequestConfig, AxiosRequestHeaders, AxiosResponse} from "axios";
import { components } from "./openapi_schemas";
import { useContext, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import UserContext from "./context/user";


const  REFRESH_TOKEN_KEY = "refresh_token";
const  ACCESS_TOKEN_KEY = "access_token";

var axiosInstance = axios.create();

export const search = async (
  apiURL: string,
  segments: string[],
  signal: AbortSignal,
  fields: string[] = [],
  selectMetadata: any = null,
  pageOffset: number = 0,
  pageLimit: number = 100
): Promise<
  components["schemas"]["Response_List_tiled.server.router.Resource_NodeAttributes__dict__dict____PaginationLinks__dict_"]
> => {
  let url = `${apiURL}/search/${segments.join(
    "/"
  )}?page[offset]=${pageOffset}&page[limit]=${pageLimit}&fields=${fields.join(
    "&fields="
  )}`;
  if (selectMetadata !== null) {
    url = url.concat(`&select_metadata=${selectMetadata}`);
  }
  const response = await axiosInstance.get(url, { signal: signal });
  return response.data;
};

export const metadata = async (
  apiURL: string,
  segments: string[],
  signal: AbortSignal,
  fields: string[] = []
): Promise<
  components["schemas"]["Response_Resource_NodeAttributes__dict__dict___dict__dict_"]
> => {
  const response = await axiosInstance.get(
    `${apiURL}/metadata/${segments.join("/")}?fields=${fields.join("&fields=")}`,
    { signal: signal }
  );
  return response.data;
};


export const about = async (): Promise<components["schemas"]["About"]> => {
  const response = await axiosInstance.get("/");
  return response.data;
};

interface Props {
    children: any
}

// Mounts interceptor code to axios on navigation.
// inspired from https://dev.to/arianhamdi/react-hooks-in-axios-interceptors-3e1h
// This allows us to fold axios interception into scoped JSX objects.
const AxiosInterceptor = ({children}: Props) => {

  const navigate = useNavigate();
  const {user, setUser} = useContext(UserContext);

  useEffect(() => {

      const responseInterceptor = async (response: AxiosResponse) => {
          // if 401, delete local toke

          // looku
          const refreshToken = response.data.refresh_token;
          if (refreshToken){
            localStorage.setItem(REFRESH_TOKEN_KEY, refreshToken)
            localStorage.setItem(ACCESS_TOKEN_KEY, response.data.access_token);
            // follow up request to /api/whoami to get user info
            const whomiResponse = await axiosInstance.get("/auth/whoami");
            //what provider was this? this is ugly
            const provider = response.config.url!.split("/")[2];
            //now find the identity for this provider
            // I guess a types file for tiled would be could, we could avoid the any here
            const identity = whomiResponse.data.identities.find((identity: any) => identity.provider === provider);
            //update user context
            setUser(identity.id);
          }





          return response;
      }

      const requestInterceptor =  (requestConfig: AxiosRequestConfig) => {
        // for all requests, lookup refresh token in local storage
        // if found, add to the reqeust
        const storedAccessToken = localStorage.getItem(ACCESS_TOKEN_KEY);
        if (storedAccessToken){
          if (!requestConfig.headers){
            requestConfig.headers = {};
          }
          requestConfig.headers["Authorization"] = "Bearer " + storedAccessToken;
        }
        return requestConfig;
    }

      const errInterceptor = (error: any) => {

          if (error.response.status === 401) {
              navigate('/login');
          }

          return Promise.reject(error);
      }

      const reqInterceptor = axiosInstance.interceptors.request.use(requestInterceptor);
      const interceptor = axiosInstance.interceptors.response.use(responseInterceptor, errInterceptor);

      // Why does the article do this and do I need to eject the request intereptor?
      return () => axiosInstance.interceptors.response.eject(interceptor);

  }, [navigate])

  return children;
}



export { axiosInstance, AxiosInterceptor };
