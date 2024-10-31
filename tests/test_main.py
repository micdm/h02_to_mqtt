import pytest
from pytest_mock import MockerFixture

from main import handle_request


@pytest.mark.parametrize(
    "data, expected",
    (
        (b"*fir", None),
        (b"*first#*second", b"first"),
        (b"*first#*second#", b"first"),
    ),
)
def test_handle_request(mocker: MockerFixture, data: bytes, expected: bytes) -> None:
    request = mocker.Mock(recv=mocker.Mock(side_effect=(data, b"")))
    result = handle_request(request)
    assert result == expected
