from typing import Any, Iterator
from unittest.mock import MagicMock


class AwaitableNonAsyncMagicMock(MagicMock):
    def __await__(self) -> Iterator[Any]:
        return iter([])
