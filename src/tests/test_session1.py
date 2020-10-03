from src.session1.hello import add
from src.session1.hello import is_unique
from src.session1.hello import squared


def test_add():
    assert add(1, 2) == 3


def test_squared():
    assert squared([2, 4, 6]) == [4, 16, 36]


def test_is_unique():
    assert is_unique([2, 5, 9, 7])
    assert not is_unique([2, 5, 5, 7])
