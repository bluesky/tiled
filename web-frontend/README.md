# Tiled's Built-in Web Frontend

This is a generic, data-oriented web UI for Tiled. For real applications, we
encourage users to develop their own UIs tuned to their needs. This is just an
example of what is possible.

## Development workflow

Start a Tiled server in a way that allows connections from the React app. For example:

```
TILED_ALLOW_ORIGINS='["http://localhost:5173"]' tiled serve demo --public
```

```
cd web-frontend
npm install
npm run serve
```

The front-end will launch at `http://localhost:5173`.

## Packaging

The build hook in `hatch_build.py` builds the UI and copies the production
build into `share/tiled/ui`. The build will include this as _data_, included in
the distribution but placed outside the Python package.

## Generating TypeScript from OpenAPI

```
npx openapi-typescript http://localhost:8000/openapi.json --output ./src/openapi_schemas.ts
```

Docs: https://www.npmjs.com/package/openapi-typescript
