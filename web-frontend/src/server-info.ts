import { components } from "./openapi_schemas";

export type ServerInfo = components["schemas"]["Response_List_tiled.server.router.Resource_NodeAttributes__dict__dict____PaginationLinks__dict_"];



const fetchServerInfo = async (
    signal: AbortSignal,
    apiURL: string
  ): Promise<ServerInfo> => {
    const response = await fetch(apiURL, { signal });
    return await response.json() as ServerInfo;
  };
  
  export { fetchServerInfo };