
## Semantic search handler API

- Given a query for a specific file uuid, picks encoding for that file from s3 bucket. If it's a cold start, saves a local copy on disk, serves relevant matching lines
- If a different query for the same file is received, loads file from disk and returns results



### Credits to
- [Cortex.dev](https://github.com/cortexlabs/cortex) for making this simple to use API for deploying the model
