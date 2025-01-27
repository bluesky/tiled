/**
 * This file was auto-generated by openapi-typescript.
 * Do not make direct changes to the file.
 */

export interface paths {
  "/": {
    get: operations["index__get"];
  };
  "/ui": {
    get: operations["ui_ui_get"];
  };
  "/api/": {
    get: operations["about_api__get"];
  };
  "/api/metadata/{path}": {
    /** Fetch the metadata and structure information for one entry. */
    get: operations["node_metadata_api_node_metadata__path__get"];
  };
  "/api/array/block/{path}": {
    /** Fetch a chunk of array-like data. */
    get: operations["array_block_api_array_block__path__get"];
  };
  "/api/array/full/{path}": {
    /** Fetch a slice of array-like data. */
    get: operations["full_array_api_array_full__path__get"];
  };
  "/api/table/partition/{path}": {
    /** Fetch a partition (continuous block of rows) from a DataFrame. */
    get: operations["table_partition_api_table_partition__path__get"];
  };
  "/api/node/full/{path}": {
    /** Fetch the data below the given node. */
    get: operations["full_xarray_Dataset_api_node_full__path__get"];
  };
  "/api/search/{path}": {
    get: operations["node_search_api_node_search__path__get"];
  };
}

export interface components {
  schemas: {
    /** About */
    About: {
      /** Api Version */
      api_version: number;
      /** Library Version */
      library_version: string;
      /** Formats */
      formats: { [key: string]: string[] };
      /** Aliases */
      aliases: { [key: string]: { [key: string]: string[] } };
      /** Queries */
      queries: string[];
      authentication: components["schemas"]["AboutAuthentication"];
      /** Links */
      links: { [key: string]: string };
      /** Meta */
      meta: { [key: string]: unknown };
    };
    /** AboutAuthentication */
    AboutAuthentication: {
      /** Required */
      required: boolean;
      /** Providers */
      providers: components["schemas"]["AboutAuthenticationProvider"][];
      links?: components["schemas"]["AboutAuthenticationLinks"];
    };
    /** AboutAuthenticationLinks */
    AboutAuthenticationLinks: {
      /** Whoami */
      whoami: string;
      /** Apikey */
      apikey: string;
      /** Refresh Session */
      refresh_session: string;
      /** Revoke Session */
      revoke_session: string;
      /** Logout */
      logout: string;
    };
    /** AboutAuthenticationProvider */
    AboutAuthenticationProvider: {
      /** Provider */
      provider: string;
      mode: components["schemas"]["AuthenticationMode"];
      /** Links */
      links: { [key: string]: string };
      /** Confirmation Message */
      confirmation_message?: string;
    };
    /**
     * AuthenticationMode
     * @description An enumeration.
     * @enum {string}
     */
    AuthenticationMode: "internal" | "external";
    /**
     * EntryFields
     * @description An enumeration.
     * @enum {string}
     */
    EntryFields:
      | "metadata"
      | "structure_family"
      | "structure"
      | "structure"
      | "count"
      | "sorting"
      | "specs"
      | "";
    /** Error */
    Error: {
      /** Code */
      code: number;
      /** Message */
      message: string;
    };
    /** HTTPValidationError */
    HTTPValidationError: {
      /** Detail */
      detail?: components["schemas"]["ValidationError"][];
    };
    /** NodeAttributes */
    NodeAttributes: {
      /** Ancestors */
      ancestors: string[];
      structure_family?: components["schemas"]["StructureFamilies"];
      /** Specs */
      specs?: string[];
      /** Metadata */
      metadata?: { [key: string]: unknown };
      structure?: components["schemas"]["Structure"];
      /** Count */
      count?: number;
      /** Sorting */
      sorting?: components["schemas"]["SortingItem"][];
    };
    /** PaginationLinks */
    PaginationLinks: {
      /** Self */
      self: string;
      /** Next */
      next: string;
      /** Prev */
      prev: string;
      /** First */
      first: string;
      /** Last */
      last: string;
    };
    /**
     * Resource[NodeAttributes, dict, dict]
     * @description A JSON API Resource
     */
    Resource_NodeAttributes__dict__dict_: {
      /** Id */
      id: string;
      attributes: components["schemas"]["NodeAttributes"];
      /** Links */
      links?: { [key: string]: unknown };
      /** Meta */
      meta?: { [key: string]: unknown };
    };
    /** Response */
    Response: {
      /** Data */
      data?: unknown;
      error?: components["schemas"]["Error"];
      /** Links */
      links?: unknown;
      /** Meta */
      meta?: unknown;
    };
    /** Response[List[tiled.server.router.Resource[NodeAttributes, dict, dict]], PaginationLinks, dict] */
    "Response_List_tiled.server.router.Resource_NodeAttributes__dict__dict____PaginationLinks__dict_": {
      /** Data */
      data?: components["schemas"]["Resource_NodeAttributes__dict__dict_"][];
      error?: components["schemas"]["Error"];
      links?: components["schemas"]["PaginationLinks"];
      /** Meta */
      meta?: { [key: string]: unknown };
    };
    /** Response[Resource[NodeAttributes, dict, dict], dict, dict] */
    Response_Resource_NodeAttributes__dict__dict___dict__dict_: {
      data?: components["schemas"]["Resource_NodeAttributes__dict__dict_"];
      error?: components["schemas"]["Error"];
      /** Links */
      links?: { [key: string]: unknown };
      /** Meta */
      meta?: { [key: string]: unknown };
    };
    /**
     * SortingDirection
     * @description An enumeration.
     * @enum {integer}
     */
    SortingDirection: 1 | -1;
    /** SortingItem */
    SortingItem: {
      /** Key */
      key: string;
      direction: components["schemas"]["SortingDirection"];
    };
    /** Structure */
    Structure: {
      [key: string]: unknown
    };
    /**
     * StructureFamilies
     * @description An enumeration.
     * @enum {string}
     */
    StructureFamilies:
      | "container"
      | "array"
      | "table"
      | "xarray_data_array"
      | "xarray_dataset";
    /** ValidationError */
    ValidationError: {
      /** Location */
      loc: string[];
      /** Message */
      msg: string;
      /** Error Type */
      type: string;
    };
  };
}

export interface operations {
  index__get: {
    responses: {
      /** Successful Response */
      200: {
        content: {
          "text/html": string;
        };
      };
    };
  };
  ui_ui_get: {
    responses: {
      /** Successful Response */
      200: {
        content: {
          "text/html": string;
        };
      };
    };
  };
  about_api__get: {
    responses: {
      /** Successful Response */
      200: {
        content: {
          "application/json": components["schemas"]["About"];
        };
      };
    };
  };
  /** Fetch the metadata and structure information for one entry. */
  node_metadata_api_node_metadata__path__get: {
    parameters: {
      path: {
        path: string;
      };
      query: {
        fields?: components["schemas"]["EntryFields"][];
        select_metadata?: string;
        omit_links?: boolean;
        root_path?: boolean;
      };
    };
    responses: {
      /** Successful Response */
      200: {
        content: {
          "application/json": components["schemas"]["Response_Resource_NodeAttributes__dict__dict___dict__dict_"];
        };
      };
      /** Validation Error */
      422: {
        content: {
          "application/json": components["schemas"]["HTTPValidationError"];
        };
      };
    };
  };
  /** Fetch a chunk of array-like data. */
  array_block_api_array_block__path__get: {
    parameters: {
      path: {
        path: string;
      };
      query: {
        format?: string;
        block: string;
        slice?: string;
        expected_shape?: string;
      };
    };
    responses: {
      /** Successful Response */
      200: {
        content: {
          "application/json": components["schemas"]["Response"];
        };
      };
      /** Validation Error */
      422: {
        content: {
          "application/json": components["schemas"]["HTTPValidationError"];
        };
      };
    };
  };
  /** Fetch a slice of array-like data. */
  full_array_api_array_full__path__get: {
    parameters: {
      path: {
        path: string;
      };
      query: {
        format?: string;
        slice?: string;
        expected_shape?: string;
      };
    };
    responses: {
      /** Successful Response */
      200: {
        content: {
          "application/json": components["schemas"]["Response"];
        };
      };
      /** Validation Error */
      422: {
        content: {
          "application/json": components["schemas"]["HTTPValidationError"];
        };
      };
    };
  };
  /** Fetch a partition (continuous block of rows) from a DataFrame. */
  table_partition_api_table_partition__path__get: {
    parameters: {
      path: {
        path: string;
      };
      query: {
        partition: number;
        field?: string[];
        format?: string;
      };
    };
    responses: {
      /** Successful Response */
      200: {
        content: {
          "application/json": components["schemas"]["Response"];
        };
      };
      /** Validation Error */
      422: {
        content: {
          "application/json": components["schemas"]["HTTPValidationError"];
        };
      };
    };
  };
  /** Fetch the data below the given node. */
  full_xarray_Dataset_api_node_full__path__get: {
    parameters: {
      path: {
        path: string;
      };
      query: {
        field?: string[];
        format?: string;
      };
    };
    responses: {
      /** Successful Response */
      200: {
        content: {
          "application/json": components["schemas"]["Response"];
        };
      };
      /** Validation Error */
      422: {
        content: {
          "application/json": components["schemas"]["HTTPValidationError"];
        };
      };
    };
  };
  node_search_api_node_search__path__get: {
    parameters: {
      path: {
        path: string;
      };
      query: {
        fields?: components["schemas"]["EntryFields"][];
        select_metadata?: string;
        "page[offset]"?: number;
        "page[limit]"?: number;
        sort?: string;
        omit_links?: boolean;
        "filter[fulltext][condition][text]"?: string[];
        "filter[lookup][condition][key]"?: string[];
      };
    };
    responses: {
      /** Successful Response */
      200: {
        content: {
          "application/json": components["schemas"]["Response_List_tiled.server.router.Resource_NodeAttributes__dict__dict____PaginationLinks__dict_"];
        };
      };
      /** Validation Error */
      422: {
        content: {
          "application/json": components["schemas"]["HTTPValidationError"];
        };
      };
    };
  };
}

export interface external {}
