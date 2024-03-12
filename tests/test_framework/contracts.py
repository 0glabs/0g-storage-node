from os.path import join
from pathlib import Path
import json
from web3 import Web3


def load_contract_metadata(base_path: str, name: str):
    path = Path(join(base_path, "artifacts"))
    try:
        found_file = next(path.rglob(f"{name}.json"))
        return json.loads(open(found_file, "r").read())
    except StopIteration:
        raise Exception(f"Cannot found contract {name}'s metadata")

