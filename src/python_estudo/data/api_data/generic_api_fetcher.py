import requests
from loguru import logger


class APIRequest:
    """
    Generic class for handling API requests.
    """
    def __init__(self, base_url: str):
        self.base_url = base_url

    def fetch(self, endpoint: str, params: dict = None) -> dict:
        url = f"{self.base_url}/{endpoint}"
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.exception(f"API request failed for {url}: {e}")
            raise


# Example Implementation
class ExampleAPIRequest(APIRequest):
    """
    Example API implementation.
    """
    def __init__(self):
        super().__init__(base_url="https://jsonplaceholder.typicode.com")

    def fetch_posts(self):
        return self.fetch(endpoint="posts")

if __name__ == "__main__":
    example = ExampleAPIRequest()
    print(example.fetch_posts())