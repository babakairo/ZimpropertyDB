from scraper.middlewares.middlewares import (
    RotatingUserAgentMiddleware,
    ProxyMiddleware,
    DuplicateFilterMiddleware,
)

__all__ = [
    "RotatingUserAgentMiddleware",
    "ProxyMiddleware",
    "DuplicateFilterMiddleware",
]
