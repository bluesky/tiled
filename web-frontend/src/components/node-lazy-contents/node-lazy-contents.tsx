import {
  DataGrid,
  GridRowModel,
  GridRowParams,
  GridSortModel,
  GridToolbarColumnsButton,
  GridToolbarContainer,
  GridToolbarDensitySelector,
} from "@mui/x-data-grid";
import { useCallback, useContext, useEffect, useMemo, useRef, useState } from "react";

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
  const navigate = useNavigate();

  // Memoize grid column definitions — rebuilding on every render is wasteful.
  const gridColumns = useMemo(() => {
    const cols = [
      {
        field: "id",
        headerName: "ID",
        flex: 1,
        hide: !props.defaultColumns.includes("id"),
      },
    ];
    props.columns.forEach((column) =>
      cols.push({
        field: column.field,
        headerName: column.header,
        flex: 1,
        hide: !props.defaultColumns.includes(column.field),
      }),
    );
    return cols;
  }, [props.columns, props.defaultColumns]);

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
    let active = true;
    const controller = new AbortController();

    (async () => {
      setRowsState((prev) => ({ ...prev, loading: true }));

      try {
        const { fields, selectMetadata } = buildFieldParams();
        const sort = buildSort();
        const { page, pageSize } = rowsState;

        const cachedUrl = cursorCache.current.get(page);
        let data;

        if (page === 0 || cachedUrl === undefined) {
          // Page 0 or no cursor cached: use offset-based request.
          // The server converts offset→cursor internally when supported.
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
          // Use the cursor URL cached from the previous page's links.next.
          data = await searchByUrl(
            cachedUrl,
            controller.signal,
            fields,
            selectMetadata,
            sort,
          );
        }

        // Cache the next-page cursor URL if provided.
        const nextUrl = (data.links as Record<string, unknown>)?.next as string | undefined;
        if (nextUrl) {
          cursorCache.current.set(page + 1, nextUrl);
        }

        const newItems = data.data!;
        const newIdsToAncestors: IdsToAncestors = {};
        newItems.forEach(
          (item: components["schemas"]["Resource_NodeAttributes__dict__dict_"]) => {
            newIdsToAncestors[item.id as string] = item.attributes.ancestors;
          },
        );
        const newRows = newItems.map(
          (item: components["schemas"]["Resource_NodeAttributes__dict__dict_"]) => {
            const row: { [key: string]: any } = { id: item.id };
            props.columns.forEach((column) => {
              row[column.field] = item.attributes!.metadata![column.field];
            });
            return row;
          },
        );

        if (!active) return;

        setRowCount(data.meta!.count! as number);
        setIdsToAncestors(newIdsToAncestors);
        setRowsState((prev) => ({ ...prev, loading: false, rows: newRows }));
      } catch (err: any) {
        // Ignore aborts (navigation away, deps change); surface real errors.
        if (err?.name === "CanceledError" || err?.name === "AbortError") return;
        if (!active) return;
        setRowsState((prev) => ({ ...prev, loading: false }));
      }
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
            setRowsState((prev) => {
              const pageSizeChanged = pageSize !== prev.pageSize;
              if (pageSizeChanged) {
                // Reset cursor cache synchronously so the fetch effect
                // uses offset=0 rather than a stale cursor for the old page size.
                cursorCache.current = new Map();
              }
              return {
                ...prev,
                // Reset to page 0 when page size changes to avoid landing
                // at an out-of-range offset.
                page: pageSizeChanged ? 0 : page,
                pageSize,
              };
            });
          }}
          onRowClick={(params: GridRowParams) => {
            navigate(
              `/browse${idsToAncestors[params.id]
                .map((ancestor: string) => "/" + ancestor)
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
            cursorCache.current = new Map();
            setSortModel(model);
            // Reset to first page when sort changes.
            setRowsState((prev) => ({ ...prev, page: 0 }));
          }}
        />
      </Container>
    </Box>
  );
};

export default NodeLazyContents;
