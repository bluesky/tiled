# Use Tiled from Commandline (e.g. curl)

This tutorial demonstrates using Tiled with a commandline HTTP client.

To follow along, start the Tiled server with example data from a Terminal.

```
tiled serve pyobject --public tiled.examples.generated:tree
```

In another Terminal, we'll use commandline HTTP clients to access this data.
The most universal commandline HTTP client is [curl](https://curl.se/), which
you may already ahve installed. You can use that for the examples below if you
wish, but we recommend installing the modern alternative
[HTTPie](https://httie.io/).  We'll also use
[jq](https://stedolan.github.io/jq/) to parse the JSON responses. Follow those
links for installation instructions.

To start, we want to know the "table of contents" of this server, which we will find here:

```
$ http http://localhost:8000/node/search/
<large output>
```

This is a "search" with no filter---i.e. all the results. Or, to be precise, the first
_page_ of results if the number of results is large. For each entry, there are links. We
can use `jq` to extract just the links to the complete dataset for each entry:

```
$ http :8000/node/search/ | jq .data[].links.full
"http://localhost:8000/array/full/big_image"
"http://localhost:8000/array/full/small_image"
"http://localhost:8000/array/full/medium_image"
"http://localhost:8000/array/full/tiny_image"
"http://localhost:8000/array/full/tiny_cube"
"http://localhost:8000/array/full/tiny_hypercube"
"http://localhost:8000/array/full/low_entropy"
"http://localhost:8000/array/full/high_entropy"
"http://localhost:8000/node/full/short_table"
"http://localhost:8000/node/full/long_table"
null
null
"http://localhost:8000/array/full/dynamic"
```

We can follow one of those links to download the data. We should specify which
format we would like it in. The root route `/` provides general information
about this server, including which formats are supported.

```
$ http "http://localhost:8000/" | jq .formats.array
[
  "application/octet-stream",
  "application/json",
  "text/csv",
  "text/plain",
  "image/png",
  "image/tiff",
  "text/html"
]
```

```
$ http "http://localhost:8000/array/full/tiny_image" Accept:text/csv
HTTP/1.1 200 OK
content-length: 1921
content-type: text/csv; charset=utf-8
date: Mon, 10 Jan 2022 14:38:53 GMT
etag: 3089180f1a77ae10cd726e1128b92d7f
server: uvicorn
server-timing: read;dur=0.6, tok;dur=0.1, pack;dur=0.3, app;dur=5.5
set-cookie: tiled_csrf=RRccfzjrsAz9LfcqTHeCXotsjeJYIMkySNRTRYG_oOA; HttpOnly; Path=/; SameSite=lax

0.8871623177732875,0.8184238619216139,0.297813977733835,0.2243267425739428,0.3799213081647492,0.39306543599210864,0.05000393209310661,0.4928506931610588,0.26271746602105306,0.7551618984935738
0.08797967019285835,0.8769284698175998,0.9769831435423009,0.2806163016621548,0.4610899030567025,0.2846943059577941,0.8843208438834496,0.20374463729071235,0.05689557381345611,0.30891817152434276
0.7135841549821578,0.2903441259467331,0.4911821899704165,0.6205043560968119,0.49962469918461105,0.9517393592455842,0.6694537435736307,0.07233861758918958,0.8115606407832385,0.8692596907680337
0.22762075863269193,0.5975269304879889,0.20703353778057676,0.693659448423662,0.20895528998668955,0.535965307915169,0.40616710062649997,0.537040163584647,0.7829184633780695,0.33504273350088387
0.29099943504634185,0.05947673315802271,0.517677200834831,0.538336092291576,0.4316791805874789,0.009060666903294656,0.8355934014526298,0.9612815880798777,0.613872549322204,0.8982659395116971
0.2946276040395521,0.9585243152911743,0.026764307607297977,0.4255240666089133,0.9925735423314712,0.11610904997555382,0.48737230150751365,0.9654278121443973,0.35652103860313,0.22596686814779077
0.5597673871665866,0.03629195630047943,0.1497236050527051,0.8577400056528133,0.9081747722748317,0.08614585356534354,0.8570794105854995,0.10737866630043613,0.27254038519307766,0.10318002284373351
0.659515057883873,0.7275525697354209,0.4897093176012439,0.8721155862120674,0.8078846097818955,0.5313278022671419,0.9414012488573992,0.48835713381302304,0.5652764082378566,0.9739320881340974
0.7281678246064428,0.7118079723082962,0.7718850244858007,0.21079688199373958,0.47252385038297606,0.5434915375502117,0.492217985892783,0.2288878523138459,0.3712190974908137,0.8061520076288899
0.632369894322198,0.1987009802074161,0.09289063027215405,0.9211530446607924,0.2957688064950228,0.9712959033555114,0.3029106849426233,0.3990013531081682,0.02645248716957882,0.8067144389288178
```

Equivalently, we can use the `format` query parameter:

```
$ http "http://localhost:8000/array/full/tiny_image?format=text/csv"
<same output as above>
```

We can download the same data as PNG-formatted image instead, and save it to a file.

```
$ http "http://localhost:8000/array/full/tiny_image?format=image/png" > tiny_image.png
<same output as above>
```

The formats like `text/csv` or `image/png` are
[MIME types](https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types),
a standard well understood by many software programs. For human use, aliases based on
familiar file extensions like `.csv` or `.png` are available. The root route `/` provides
a list of the supported aliases as well:

```
$ http "http://localhost:8000/" | jq .aliases.array
{
  "application/json": [
    "json"
  ],
  "text/csv": [
    "csv"
  ],
  "image/png": [
    "png"
  ],
  "image/tiff": [
    "tiff",
    "tif"
  ],
  "text/html": [
    "html",
    "htm",
    "shtml"
  ],
  "application/netcdf": [
    "nc"
  ],
  "text/plain": [
    "text",
    "txt"
  ]
}
```
