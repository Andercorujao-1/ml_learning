from collections.abc import Callable
from pydantic import BaseModel, Field
from typing_extensions import Literal

# Define allowed HTTP methods for endpoints
HttpMethod = Literal["GET", "POST", "PUT", "DELETE"]

class ApiRequestModel(BaseModel):
    url: str
    endpoint: str
    method: HttpMethod = "GET"
    parameters: dict[str, str] | None = None
    retryCount: int = 0
    # Map error codes to handler functions (if any), excluded from JSON.
    errorMapping: dict[int, Callable[['ApiRequestModel'], None]] = Field(default_factory=dict, exclude=True)

if __name__ == "__main__":
    def handle_413(config: ApiRequestModel) -> None:
        print(f"Handling error 413 for endpoint {config.endpoint} with retryCount {config.retryCount}")

    # Create a sample API request configuration
    config = ApiRequestModel(
        url="https://api.example.com",
        endpoint="weather",
        method="GET",
        parameters={"lat": "0", "lon": "0"},
        errorMapping={413: handle_413}
    )
    
    # Simulate receiving an error 413 and invoke its handler.
    if handler := config.errorMapping.get(413):
        handler(config)
    else:
        print("No handler found for error code 413.")
