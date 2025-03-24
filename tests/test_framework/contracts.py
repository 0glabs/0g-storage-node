from pathlib import Path
import json


def load_contract_metadata(path: str, name: str):
    path = Path(path)
    try:
        found_file = next(path.rglob(f"{name}.json"))
        return json.loads(open(found_file, "r").read())
    except StopIteration:
        raise Exception(f"Cannot found contract {name}'s metadata")
