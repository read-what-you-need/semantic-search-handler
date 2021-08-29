
## Semantic search handler API

### Post request


#### Example

```
{
	"uuid": "d74ea1c8-6dbc-4e12-8d71-7c0c3d23b754",
	"text": "What is the way ?",
	"top": 5,
	"accuracyGreaterThan": 0.2
}

```

#### Parameters usage

##### `uuid` (string)
The file that we want to search in for.

##### `text` (string)
The search query

##### `top` (integer)
Get the top N search results for our query

##### `accuracyGreaterThan` (float)
Only get those results whose accuracy is greater than.


### About API


- Given a query for a specific file uuid, picks encoding for that file from s3 bucket. If it's a cold start, saves a local copy on disk, serves relevant matching lines
- If a different query for the same file is received, loads file from disk and returns results


### Credits to
- [Cortex.dev](https://github.com/cortexlabs/cortex) for making this simple to use API for deploying the model
