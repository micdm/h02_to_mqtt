from collections.abc import Sequence

import pytest
from pytest_mock import MockerFixture

from main import handle_request


@pytest.mark.parametrize(
    "data, expected",
    (
        (b"*fir", ()),
        (b"*first#*second", (b"first",)),
        (b"*first#*second#", (b"first", b"second")),
    ),
)
def test_handle_request(
    mocker: MockerFixture, data: bytes, expected: Sequence[bytes]
) -> None:
    request = mocker.Mock(recv=mocker.Mock(side_effect=(data, b"")))
    result = handle_request(request)
    assert tuple(result) == expected
