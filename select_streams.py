import os
import sys
import json
import argparse
from typing import Sequence, List, Dict, Union, Any
from functools import reduce

JSONType = Union[List['JSONType'], Dict[str, 'JSONType']]


def get_streams(catalog: JSONType):
    return [stream["stream"] for stream in catalog["streams"]]


def xor(*args: Any) -> bool:
    return reduce(lambda a, b: a ^ b, (bool(arg) for arg in args))


def save_catalog(catalog: JSONType, file_name: str):
    file_name = os.path.join("/tmp", file_name)
    with open(file_name, "w") as f:
        json.dump(catalog, f)
    print(file_name)


def detect_unknown_streams(stream_names: Sequence[str], input_names: Sequence[str]):
    unknown_names = set(input_names) - set(stream_names)
    if unknown_names:
        raise ValueError(f"Warning: Streams you have referenced do not exist in the catalog!\n"
                         f"unknown streams: " + ", ".join(unknown_names))


def filter_catalog(catalog: Dict, select: Sequence[str] = None, exclude: Sequence[str] = None):
    for stream in catalog["streams"]:
        for mdata in stream["metadata"]:
            if mdata.get("breadcrumb") == []:
                if select is not None:
                    mdata["metadata"]["selected"] = stream["stream"] in select
                elif exclude is not None:
                    mdata["metadata"]["selected"] = stream["stream"] not in exclude


def remove_schema(catalog: Dict):
    for stream in catalog["streams"]:
        stream["schema"] = {}


def main(catalog, select, exclude, mode, should_remove_schema):
    detect_unknown_streams((s["stream"] for s in catalog["streams"]), select or exclude)

    if should_remove_schema:
        remove_schema(catalog)

    filter_catalog(catalog, select, exclude)

    if mode == "file":
        file_name = "temp_catalog"
        if select:
            file_name += "__select-" + "-".join(select)
        if exclude:
            file_name += "__excluded-" + "-".join(exclude)
        file_name += ".json"
        save_catalog(catalog, file_name)

    elif mode == "print":
        print(json.dumps(catalog))


if __name__ == "__main__":
    parser = argparse.ArgumentParser("select and de-select streams from singer catalog",
                                     epilog="saves output to tmp and gives file path to print")

    parser.add_argument("FILE", nargs="?", help="catalog file")
    parser.add_argument("-s", "--select", nargs="*", help="names of streams to select")
    parser.add_argument("-x", "--exclude", "--delete", nargs="*",
                        help="names of streams to de-select (complement to -s)")

    parser.add_argument("--mode", default="file", choices=["file", "print"], help="how to return result")
    parser.add_argument("-a", "--all", action="store_true", help="select all streams")
    parser.add_argument("--list", action="store_true", help="list streams in catalog")

    parser.add_argument("--remove-schema", action="store_true", help="leave only '{}' schema for all streams")

    args = parser.parse_args()

    with open(args.FILE) as f:
        catalog = json.load(f)

    if args.list:
        print("\n".join(get_streams(catalog)))
        sys.exit()

    if not xor(args.select, args.exclude, args.all):
        raise ValueError("specify only one of -s, -d and --all")

    select = get_streams(catalog) if args.all else args.select

    main(catalog, select, args.exclude, args.mode, args.remove_schema)