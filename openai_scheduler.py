import asyncio
import logging
import signal
import time
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, List, Optional

try:
    import openai
except ImportError:  # pragma: no cover - openai may not be installed
    openai = None


@dataclass
class SchedulerConfig:
    max_concurrent: int = 15
    send_interval: float = 4.0
    backoff_start: float = 1.0
    max_retries: int = 3
    api_key: Optional[str] = None
    progress_file: Optional[Path] = None
    model: str = "gpt-3.5-turbo"
    temperature: float = 1.0
    top_p: float = 1.0
    presence_penalty: float = 0.0
    frequency_penalty: float = 0.0
    max_tokens: Optional[int] = None


class OpenAIScheduler:
    """Schedule OpenAI requests with rate limiting and retry logic."""

    def __init__(self, config: SchedulerConfig, result_handler: Callable[[int, Any], None]):
        self.config = config
        self.result_handler = result_handler
        self.semaphore = asyncio.Semaphore(config.max_concurrent)
        self._send_lock = asyncio.Lock()
        self._last_send = 0.0
        self._stop = asyncio.Event()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.stats = {
            "total": 0,
            "errors": 0,
            "errors_429": 0,
            "latency_over_6s": 0,
            "token_usage": 0,
        }
        self.progress_index = self._load_progress()

    def _load_progress(self) -> int:
        if self.config.progress_file and self.config.progress_file.exists():
            try:
                return int(self.config.progress_file.read_text().strip())
            except Exception:
                return 0
        return 0

    def _save_progress(self, index: int) -> None:
        if self.config.progress_file:
            self.config.progress_file.write_text(str(index))

    async def _rate_limit(self) -> None:
        async with self._send_lock:
            now = time.monotonic()
            wait_time = self.config.send_interval - (now - self._last_send)
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            self._last_send = time.monotonic()

    async def _send_request(self, chunk: str) -> Any:
        if openai is None:
            await asyncio.sleep(0.1)
            return {"choices": [{"message": {"content": f"Echo: {chunk[:20]}"}}], "usage": {"total_tokens": len(chunk.split())}}
        else:
            params = {
                "model": self.config.model,
                "messages": [{"role": "user", "content": chunk}],
                "api_key": self.config.api_key,
                "temperature": self.config.temperature,
                "top_p": self.config.top_p,
                "presence_penalty": self.config.presence_penalty,
                "frequency_penalty": self.config.frequency_penalty,
            }
            if self.config.max_tokens is not None:
                params["max_tokens"] = self.config.max_tokens
            return await openai.ChatCompletion.acreate(**params)

    async def _process_chunk(self, index: int, chunk: str) -> None:
        self.stats["total"] += 1
        retries = 0
        while retries <= self.config.max_retries and not self._stop.is_set():
            try:
                if retries == 0:
                    await self._rate_limit()
                self.logger.info("Sending chunk %s attempt %s", index, retries + 1)
                start = time.monotonic()
                response = await self._send_request(chunk)
                latency = time.monotonic() - start
                if latency > 6:
                    self.stats["latency_over_6s"] += 1
                usage = getattr(response, "usage", {}).get("total_tokens", 0)
                self.stats["token_usage"] += usage
                self.logger.info(
                    "Chunk %s succeeded in %.2fs using %s tokens",
                    index,
                    latency,
                    usage,
                )
                self.result_handler(index, response)
                self._save_progress(index + 1)
                return
            except Exception as exc:  # handle openai errors generically
                self.stats["errors"] += 1
                code = getattr(exc, "status_code", None)
                if code == 429 or "429" in str(exc):
                    self.stats["errors_429"] += 1
                    if retries >= self.config.max_retries:
                        self.logger.error("Chunk %s failed with 429 after retries", index)
                        break
                    backoff = self.config.backoff_start * (2 ** retries)
                    self.logger.warning(
                        "429 on chunk %s, retry in %.1fs", index, backoff
                    )
                    await asyncio.sleep(backoff)
                    retries += 1
                else:
                    if retries >= self.config.max_retries:
                        self.logger.error("Chunk %s failed: %s", index, exc)
                        break
                    backoff = self.config.backoff_start * (2 ** retries)
                    self.logger.warning(
                        "Error on chunk %s, retry in %.1fs (%s)", index, backoff, exc
                    )
                    await asyncio.sleep(backoff)
                    retries += 1
        self.logger.error("Abandon chunk %s", index)

    async def run(self, chunks: List[str]) -> None:
        loop = asyncio.get_running_loop()
        stop_signals = {signal.SIGTERM, signal.SIGINT}
        for sig in stop_signals:
            loop.add_signal_handler(sig, self._stop.set)
        tasks = []
        for idx, chunk in enumerate(chunks):
            if idx < self.progress_index:
                continue
            if self._stop.is_set():
                break
            await self.semaphore.acquire()
            task = asyncio.create_task(self._process_chunk(idx, chunk))
            task.add_done_callback(lambda t: self.semaphore.release())
            tasks.append(task)
        if tasks:
            await asyncio.gather(*tasks)
        self._alert_if_needed()

    def _alert_if_needed(self) -> None:
        if self.stats["total"] == 0:
            return
        rate_429 = self.stats["errors_429"] / self.stats["total"]
        if rate_429 > 0.1:
            self.logger.warning("ALERT: more than 10%% of chunks returned 429")
        if self.stats["latency_over_6s"] / self.stats["total"] > 0.1:
            self.logger.warning("ALERT: more than 10%% of chunks had latency >6s")


def default_handler(index: int, response: Any) -> None:
    path = Path(f"result_{index}.json")
    if hasattr(response, "choices"):
        content = response.choices[0].message.content  # type: ignore
    else:
        content = response
    path.write_text(json.dumps({"index": index, "content": str(content)}))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run OpenAI requests with rate limiting")
    parser.add_argument("chunks", help="Path to a text file with one chunk per line")
    parser.add_argument("--api-key", dest="api_key", help="OpenAI API key")
    parser.add_argument("--max-concurrent", type=int, default=15)
    parser.add_argument("--interval", type=float, default=4.0, help="Seconds between requests")
    parser.add_argument("--backoff", type=float, default=1.0, help="Initial backoff for retries")
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--progress", type=Path, default=Path("progress.txt"))
    parser.add_argument("--model", type=str, default="gpt-3.5-turbo")
    parser.add_argument("--temperature", type=float, default=1.0)
    parser.add_argument("--top-p", type=float, default=1.0)
    parser.add_argument("--presence-penalty", type=float, default=0.0)
    parser.add_argument("--frequency-penalty", type=float, default=0.0)
    parser.add_argument("--max-tokens", type=int)

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(message)s")

    with open(args.chunks, "r", encoding="utf-8") as f:
        chunks = [line.strip() for line in f if line.strip()]

    config = SchedulerConfig(
        max_concurrent=args.max_concurrent,
        send_interval=args.interval,
        backoff_start=args.backoff,
        max_retries=args.retries,
        api_key=args.api_key,
        progress_file=args.progress,
        model=args.model,
        temperature=args.temperature,
        top_p=args.top_p,
        presence_penalty=args.presence_penalty,
        frequency_penalty=args.frequency_penalty,
        max_tokens=args.max_tokens,
    )

    scheduler = OpenAIScheduler(config, default_handler)
    asyncio.run(scheduler.run(chunks))
