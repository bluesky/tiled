import {
  DataGrid,
  GridRowModel,
  GridRowParams,
  GridSortModel,
  GridToolbarColumnsButton,
  GridToolbarContainer,
  GridToolbarDensitySelector,
} from "@mui/x-data-grid";
import { useCallback, useContext, useEffect, useRef, useState } from "react";

import Box from "@mui/material/Box";
import Container from "@mui/material/Container";
import { components } from "../../openapi_schemas";
import { search, searchByUrl } from "../../client";
import { useNavigate } from "react-router-dom";
import { SettingsContext } from "../../context/settings";

interface Column {
  header: string;
  field: string;
  select_metadata: string;
}

function CustomToolbar() {
  return (
    // working around https://github.com/mui/mui-x/issues/2383
    <GridToolbarContainer>
      <GridToolbarColumnsButton />
      <GridToolbarDensitySelector />
    </GridToolbarContainer>
  );
}

interface RowsState {
  page: number;
  pageSize: number;
  rows: GridRowModel[];
  loading: boolean;
}

interface NodeLazyContentsProps {
  segments: string[];
  columns: Column[];
  defaultColumns: string[];
  specs: string[];
}

const DEFAULT_PAGE_SIZE = 10;

const NodeLazyContents: React.FunctionComponent<NodeLazyContentsProps> = (
  props,
) => {
  let navigate = useNavigate();
  const gridColumns = [
    {
      field: "id",
      headerName: "ID",
      flex: 1,
      hide: !props.defaultColumns.includes("id"),
    },
  ];
  props.columns.map((column) =>
    gridColumns.push({
      field: column.field,
      headerName: column.header,
      flex: 1,
      hide: !props.defaultColumns.includes(column.field),
    }),
  );
  const settings = useContext(SettingsContext);
  const [rowsState, setRowsState] = useState<RowsState>({
    page: 0,
    pageSize: DEFAULT_PAGE_SIZE,
    rows: [],
    loading: false,
  });
  const [sortModel, setSortModel] = useState<GridSortModel>([]);
  type IdsToAncestors = { [key: string]: string[] };
  const [idsToAncestors, setIdsToAncestors] = useState<IdsToAncestors>({});
  const [rowCount, setRowCount] = useState<number>(0);

  // Cache of cursor URLs keyed by page index. Page 0 always uses the
  // offset-based URL; subsequent pages use the `links.next` cursor URL
  // returned by the previous page's response.
  // Reset whenever segments, sort order, or page size changes.
  const cursorCache = useRef<Map<number, string>>(new Map());

  const buildFieldParams = useCallback(() => {
    if (props.columns.length === 0) {
      return { fields: [] as string[], selectMetadata: null as string | null };
    }
    return {
      fields: ["metadata"],
      selectMetadata:
        "{" +
        props.columns
          .map((column) => `${column.field}:${column.select_metadata}`)
          .join(",") +
        "}",
    };
  }, [props.columns]);

  const buildSort = useCallback(() => {
    if (sortModel.length === 0) return null;
    return sortModel
      .map((item) => (item.sort === "desc" ? `-${item.field}` : item.field))
      .join(",");
  }, [sortModel]);

  useEffect(() => {
    // Reset cursor cache whenever anything that affects the query changes
    // (segments, sort, page size). Page number changes alone do NOT reset
    // the cache — that's the whole point.
    cursorCache.current = new Map();
  }, [props.segments, sortModel, rowsState.pageSize, props.columns]);

  useEffect(() => {
    let active = true;
    const controller = new AbortController();

    async function loadItems(): Promise<
      components["schemas"]["Resource_NodeAttributes__dict__dict_"][]
    > {
      const { fields, selectMetadata } = buildFieldParams();
      const sort = buildSort();
      const { page, pageSize } = rowsState;

      let data;
      const cachedUrl = cursorCache.current.get(page);

      if (page === 0 || cachedUrl === undefined) {
        // Page 0 or no cursor cached: use offset-based request.
        // The server will convert offset→cursor internally if it supports it.
        data = await search(
          settings.api_url,
          props.segments,
          controller.signal,
          fields,
          selectMetadata,
          page * pageSize,
          pageSize,
          sort,
        );
      } else {
        // Use the cursor URL cached from the previous page's links.next
        data = await searchByUrl(
          cachedUrl,
          controller.signal,
          fields,
          selectMetadata,
          sort,
        );
      }

      // Cache the next-page cursor URL if provided
      const nextUrl = (data.links as any)?.next as string | undefined;
      if (nextUrl) {
        cursorCache.current.set(page + 1, nextUrl);
      }

      setRowCount(data.meta!.count! as number);
      return data.data!;
    }

    (async () => {
      setRowsState((prev) => ({ ...prev, loading: true }));
      const newItems = await loadItems();

      const idsToAncestors: IdsToAncestors = {};
      newItems.map(
        (
          item: components["schemas"]["Resource_NodeAttributes__dict__dict_"],
        ) => {
          idsToAncestors[item.id as string] = item.attributes.ancestors;
          return null;
        },
      );
      const newRows = newItems.map(
        (
          item: components["schemas"]["Resource_NodeAttributes__dict__dict_"],
        ) => {
          const row: { [key: string]: any } = {};
          row.id = item.id;
          props.columns.map((column) => {
            row[column.field] = item.attributes!.metadata![column.field];
            return null;
          });
          return row;
        },
      );

      if (!active) {
        return;
      }

      setIdsToAncestors(idsToAncestors);
      setRowsState((prev) => ({ ...prev, loading: false, rows: newRows }));
    })();

    return () => {
      active = false;
      controller.abort();
    };
  }, [rowsState.page, rowsState.pageSize, props.columns, props.segments, sortModel, settings.api_url, buildFieldParams, buildSort]);

  return (
    <Box sx={{ my: 4 }}>
      <Container maxWidth="lg">
        <DataGrid
          columns={gridColumns}
          pagination
          rowCount={rowCount}
          {...rowsState}
          // Controlled pagination model to keep footer rows-per-page in sync
          paginationModel={{ page: rowsState.page, pageSize: rowsState.pageSize }}
          paginationMode="server"
          pageSizeOptions={[10, 30, 100]}
          onPaginationModelChange={({
            page,
            pageSize,
          }: {
            page: number;
            pageSize: number;
          }) => {
            setRowsState((prev) => ({
              ...prev,
              // Reset to page 0 when page size changes to avoid landing
              // at an out-of-range page (e.g. page 5 of 10-item pages
              // becomes page 5 of 100-item pages = offset 500).
              page: pageSize !== prev.pageSize ? 0 : page,
              pageSize,
            }));
          }}
          onRowClick={(params: GridRowParams) => {
            navigate(
              `/browse${idsToAncestors[params.id]
                .map(function (ancestor: string) {
                  return "/" + ancestor;
                })
                .join("")}/${params.id}`,
            );
          }}
          slots={{
            toolbar: CustomToolbar,
          }}
          disableColumnFilter
          autoHeight
          sortingMode="server"
          sortModel={sortModel}
          onSortModelChange={(model) => {
            setSortModel(model);
            // Reset to first page when sort changes to avoid confusing UX
            setRowsState((prev) => ({ ...prev, page: 0 }));
          }}
        />
      </Container>
    </Box>
  );
};

export default NodeLazyContents;
