from __future__ import annotations

import os
import sys
import json
import argparse
from typing import Sequence, Any, Optional, TypedDict
from functools import reduce


StreamMetadataClass = TypedDict("StreamMetadataClass",{
    "breadcrumb": list,
    "metadata": dict[str, bool | str | dict],
    "replication-method": Optional[str],
    "forced-replication-method": Optional[str],
})

StreamType = TypedDict("StreamType", {
    "stream": str,
    "tap_stream_id": Optional[str],
    "replication_method": Optional[str],
    "key_properties": Optional[list[str]],
    "schema": dict[str, str | list | dict],
    "metadata": list[StreamMetadataClass],
    "replication-method": Optional[str],
    "forced-replication-method": Optional[str],
})


class CatalogType(TypedDict):
    streams: list[StreamType]


def get_stream_names(catalog: CatalogType) -> list[str]:
    return [stream["stream"] for stream in catalog["streams"]]


def xor(*args: Any) -> bool:
    return reduce(lambda a, b: a ^ b, (bool(arg) for arg in args))


def save_catalog(catalog: CatalogType, file_name: str) -> None:
    file_name = os.path.join("/tmp", file_name)
    with open(file_name, "w") as f:
        json.dump(catalog, f)
    print(file_name)
    return None


def detect_unknown_streams(stream_names: Sequence[str], input_names: Sequence[str]) -> None:
    unknown_names = set(input_names) - set(stream_names)
    if unknown_names:
        raise ValueError(f"Warning: Streams you have referenced do not exist in the catalog!\n"
                         f"unknown streams: " + ", ".join(unknown_names))
    return None


def filter_catalog(catalog: CatalogType,
                   selected: Sequence[str] | None = None,
                   excluded: Sequence[str] | None = None,
                   replication_method: str | None = None,
                   forced_replication_method: str | None = None) -> None:
    for stream in catalog["streams"]:
        if replication_method:
            stream["replication-method"] = replication_method   # some taps use this location in the document
        if forced_replication_method:
            stream["forced-replication-method"] = forced_replication_method

        for mdata in stream["metadata"]:
            if not mdata.get("breadcrumb"):
                if selected is not None:
                    mdata["metadata"]["selected"] = stream["stream"] in selected
                elif excluded is not None:
                    mdata["metadata"]["selected"] = stream["stream"] not in excluded

                if replication_method:
                    mdata["metadata"]["replication-method"] = replication_method
                if forced_replication_method:
                    mdata["metadata"]["forced-replication-method"] = forced_replication_method

    return None


def remove_schema(catalog: dict):
    for stream in catalog["streams"]:
        stream["schema"] = {}


def print_stream_summary(catalog: dict):
    print(f"| {'stream':<30} | {'selected':^10} | {'selected-by-default':^20} | {'replication-method':^20} |")
    print(f"+-{'':-^30}-+-{'':-^10}-+-{'':-^20}-+-{'':-^20}-+")

    for stream in catalog["streams"]:
        metadata = [m for m in stream['metadata'] if m.get('breadcrumb') == []][0]["metadata"]
        print(f"| {stream['stream']:<30} | "
              f"{metadata.get('selected')!s:^10} | "
              f"{metadata.get('selected-by-default')!s:^20} | "
              f"{metadata.get('replication-method')!s:^20} |"
              )

    print(f"+-{'':-^30}-+-{'':-^10}-+-{'':-^20}-+-{'':-^20}-+")


def main(argv: Sequence[str] | None = None) -> int:
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
    parser.add_argument("--replication-method", choices=["INCREMENTAL", "FULL_TABLE", "LOG_BASED"],
                        help="sync streams with this method")
    parser.add_argument("--forced-replication-method", choices=["INCREMENTAL", "FULL_TABLE", "LOG_BASED"],
                        help="sync streams with this method")

    args = parser.parse_args(argv)

    if args.FILE:
        with open(args.FILE) as f:
            catalog = json.load(f)
    else:
        catalog = json.loads(sys.stdin.read())

    if args.list:
        print_stream_summary(catalog)
        return 0

    if not xor(args.select, args.exclude, args.all):
        raise ValueError("specify only one of -s, -d and --all")

    select = get_stream_names(catalog) if args.all else args.select

    detect_unknown_streams(get_stream_names(catalog), select or args.exclude)

    if args.remove_schema:
        remove_schema(catalog)

    filter_catalog(catalog, select, args.exclude, args.replication_method, args.forced_replication_method)

    if args.mode == "file":
        file_name = "temp_catalog"
        if select:
            file_name += "__select-" + "-".join(select)
        if args.exclude:
            file_name += "__excluded-" + "-".join(args.exclude)
        file_name += ".json"
        save_catalog(catalog, file_name)

    else:
        print(json.dumps(catalog))

    return 0


if __name__ == "__main__":
    exit(main())
