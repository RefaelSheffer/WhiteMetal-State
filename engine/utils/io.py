import json
from pathlib import Path
from typing import Iterable, Mapping, Union

PathLike = Union[str, Path]


def ensure_parent(path: PathLike) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def write_json(path: PathLike, data: Mapping) -> None:
    path_obj = ensure_parent(path)
    path_obj.write_text(json.dumps(data, indent=2))


def write_jsonl(path: PathLike, records: Iterable[Mapping]) -> None:
    path_obj = ensure_parent(path)
    lines = [json.dumps(record) for record in records]
    path_obj.write_text("\n".join(lines))
