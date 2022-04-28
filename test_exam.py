import json
from exam import get_symbols, Results


def test_get_symbols():
    symbols = get_symbols()
    assert symbols

    with open("symbols.json", "w") as f:
        json.dump(symbols, f, indent=2)


def test_serializer():
    with open("test-symbols.json", "r") as f:
        j = json.load(f)
        sym: Results = Results.parse_obj(j)
        assert sym
