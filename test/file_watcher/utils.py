from collections.abc import Iterator
from typing import Any
from unittest.mock import MagicMock


class AwaitableNonAsyncMagicMock(MagicMock):
    def __await__(self) -> Iterator[Any]:
        return iter([])
