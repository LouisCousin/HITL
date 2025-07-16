# HITL

This repository contains tools for PDF analysis and OpenAI interaction.

## OpenAI Scheduler

`openai_scheduler.py` provides a rate limited pipeline for sending chunks to the OpenAI API. Features include:

- **Cadence control**: one request every 4 seconds by default (15/minute).
- **Parallelism**: up to 15 concurrent chunks processed simultaneously.
- **Retry strategy**: exponential backoff on HTTP 429 or other errors with up to three retries. Retries bypass the regular 4&nbsp;s cadence so they are attempted as soon as the backoff expires.
- **Verbose logging**: each send, success and retry is logged for traceability.
- **Metrics and alerts**: logs latency, token usage and warns if more than 10% of chunks hit 429 or exceed 6 s latency.
- **Graceful shutdown**: stops new submissions on `SIGINT`/`SIGTERM` and waits for running tasks.
- **Progress tracking**: progress is stored in a file so the pipeline can resume without reprocessing completed chunks. Simply keep the progress file when restarting to skip already processed lines.

### Usage

Prepare a text file where each line is a chunk to send. Then run:

```bash
python openai_scheduler.py chunks.txt --api-key YOUR_KEY
```

Optional parameters:

- `--max-concurrent` number of active chunks (default 15)
- `--interval` seconds between requests (default 4)
- `--backoff` initial backoff delay in seconds (default 1)
- `--retries` number of retries (default 3)
- `--progress` path to the progress file (default `progress.txt`)

Results for each chunk are saved as `result_<index>.json`. To restart the pipeline without repeating finished chunks, keep the progress file.
