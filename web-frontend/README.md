# Tiled's Built-in Web Frontend

This is a generic, data-oriented web UI for Tiled.
For real applications, we encourage users to develop their
own UIs tuned to their needs. This is just an example
of what is possible.

## Development workflow

Start a Tiled server in a way that allows connections from the React app. For example:

```
TILED_ALLOW_ORIGINS=http://localhost:3000 tiled serve pyobject --public tiled.examples.generated:tree
```

Start the development server, setting the environment variable
`REACT_APP_API_PREFIX` to point to the Tiled server's `/api` route.

```
cd web-frontend
REACT_APP_API_PREFIX=http://localhost:8000/api npm start
```

The front-end will launch at `http://localhost:3000`.

## Packaging

This builds the package and copies the built assets to
`<repostiory root>/share/tiled/ui` where they can
be found and served by the FastAPI application.

```
cd web-frontend/
npm run build:pydist
```

Now a normal

```
python setup.py sdist bdist_wheel
```

will include the built application.

## Generating TypeScript from OpenAPI

```
npx openapi-typescript http://localhost:8000/openapi.json --output ./src/openapi_schemas.ts
```

Docs: https://www.npmjs.com/package/openapi-typescript
