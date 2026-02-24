# Operations Guide

## Sync Strategy in Multi-Node Deployments

The filesystem watcher is designed for single-node development. In a multi-node
or containerized deployment, local file events do not propagate across nodes.

**Recommended strategy:**

1. **Disable the watcher on all API nodes**
   - Set `SYNC_WATCHER_ENABLED=false`.
2. **Persist DataPoints in a shared store**
   - Store approved DataPoints in the system database or an object store (S3,
     GCS, etc.) and mirror them to disk as part of a background job.
3. **Trigger sync explicitly**
   - Use `POST /api/v1/sync` after any DataPoint change.
   - Run a periodic job (cron/queue) to reconcile and re-sync if needed.

This keeps vector store and knowledge graph consistent across instances and
avoids missed updates when nodes scale horizontally.
