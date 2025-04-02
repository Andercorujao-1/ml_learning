import asyncio
import logging

from emater_data_science.data.api_data.generic_api_fetcher import GenericApiFetcher
from emater_data_science.data.api_data.api_request_model import ApiRequestModel

class ApiRequestsManager:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(ApiRequestsManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, "_initialized") and self._initialized:
            return
        self._initialized = True
        # Map each URL to its own asyncio.Queue of ApiRequestConfig objects.
        self.queues: dict[str, asyncio.Queue] = {}
        # Map each URL to the task that processes its queue.
        self.tasks: dict[str, asyncio.Task] = {}
        # Global flag to signal shutdown.
        self.stop_flag = False
        self.fetcher = GenericApiFetcher()

    def fEnqueueRequest(self, config: ApiRequestModel) -> None:
        """
        Enqueues a new API request. If no queue exists for the URL,
        it creates one and starts its processing task.
        """
        url = config.url
        if url not in self.queues:
            self.queues[url] = asyncio.Queue()
            self.tasks[url] = asyncio.create_task(self._process_queue(url))
        self.queues[url].put_nowait(config)

    async def _process_queue(self, url: str) -> None:
        """
        Processes requests from the queue for the given URL sequentially.
        Before processing each request, it checks if a shutdown was signaled.
        When done, it removes the URL from the manager's maps.
        """
        queue: asyncio.Queue = self.queues[url]
        while not self.stop_flag:
            try:
                # Wait for a request with a short timeout.
                config = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                # If shutdown was signaled and no more requests are waiting, exit.
                if self.stop_flag and queue.empty():
                    break
                continue
            try:
                # Process the request asynchronously.
                await self.fetcher.fFetch(config)
            except Exception as e:
                logging.error(f"Error processing request for URL {url}, endpoint {config.endpoint}: {e}")
            finally:
                queue.task_done()
        # Remove the URL's queue and task when done.
        self.queues.pop(url, None)
        self.tasks.pop(url, None)

    async def fShutdown(self) -> None:
        """
        Signals shutdown, stops processing new requests, logs any pending requests,
        cancels in-flight tasks, and clears the manager state.
        """
        self.stop_flag = True
        # Log and drain pending requests for each URL.
        for url, queue in self.queues.items():
            pending: list[ApiRequestModel] = []
            while not queue.empty():
                try:
                    req = queue.get_nowait()
                    pending.append(req)
                    queue.task_done()
                except asyncio.QueueEmpty:
                    break
            if pending:
                logging.warning(f"Pending requests for {url}: {[req.dict() for req in pending]}")
        # Cancel all processing tasks.
        for task in self.tasks.values():
            task.cancel()
        # Wait for tasks to cancel.
        await asyncio.gather(*self.tasks.values(), return_exceptions=True)
        # Clear internal state.
        self.queues.clear()
        self.tasks.clear()

# Minimal test example.
if __name__ == "__main__":
    async def main():
        logging.basicConfig(level=logging.INFO)
        manager = ApiRequestsManager()

        # Create sample requests for two different URLs.
        config1 = ApiRequestModel(
            url="https://api.example.com",
            endpoint="weather",
            method="GET",
            parameters={"lat": "0", "lon": "0"},
            errorMapping={}
        )
        config2 = ApiRequestModel(
            url="https://api.example.com",
            endpoint="forecast",
            method="GET",
            parameters={"lat": "0", "lon": "0"},
            errorMapping={}
        )
        config3 = ApiRequestModel(
            url="https://anotherapi.example.com",
            endpoint="data",
            method="GET",
            parameters={"id": "123"},
            errorMapping={}
        )

        # Enqueue the requests.
        manager.fEnqueueRequest(config1)
        manager.fEnqueueRequest(config2)
        manager.fEnqueueRequest(config3)

        # Allow some time for processing.
        await asyncio.sleep(5)
        await manager.fShutdown()

    asyncio.run(main())
    from emater_data_science.data.data_interface import DataInterface
    DataInterface().fShutdown()
