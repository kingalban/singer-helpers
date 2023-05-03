import argparse
from tree_grafter import deep_getitem, JSONLike, ReplaceNode, apply_transformations, PathType
from tree_grafter.openAPI import parse_openAPI_doc, hide_pagination, remove_excess_keys, is_property, add_nulls
from tree_grafter.utils import y_print, j_print, pipe
from tree_grafter.depth import limit_depth
from typing import Callable, Optional, Any, Union, List, Dict, TypeVar
import yaml
import json
import sys
import os


def log(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def fits_tap_requirements(_tree, path, node) -> None:
    if is_property(node):
        assert "null" in node["type"], f"property at path={'.'.join(path)!r} has only 'null' type {node}"

    elif isinstance(node, dict) and node == {}:
        assert len(path) > 3, f"null schema at path={'.'.join(path)!r} too close to root"


def alert_empty_dict(_tree, path, node) -> None:
    if node == {}:
        print(f"found empty node at {'.'.join(path)!r}")


def main(argv) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--openAPI-file", help="path to openAPI .yml doc")
    parser.add_argument("--path-to-schema", help="dot separated path to the schema in the .yml after $ref expansion "
                                                 "(eg: paths./accounts.get.responses.200.content.application/json.schema")
    parser.add_argument("--schema-file", help="path to the schema .json file")

    args = parser.parse_args()

    with open(args.openAPI_file) as f:
        API_doc = yaml.safe_load(f.read())

    parsed_API_doc = pipe(API_doc,
                          parse_openAPI_doc,
                          apply_transformations(hide_pagination))

    documentation_schema = deep_getitem(parsed_API_doc, args.path_to_schema)

    with open(args.schema_file) as f:
        input_schema = json.load(f)

    missing_paths = []

    def fill_empty_node(_tree, path, node) -> None:
        if node == {}:
            path_str = '.'.join(path)
            try:
                replacement = deep_getitem(documentation_schema, path)
                log(f"empty node with replacement at {path_str!r})")
                return ReplaceNode(replacement, 0)
            except (IndexError, KeyError) as e:
                # log(f"empty node with NO replacement at {path_str!r}")
                missing_paths.append(path_str)
            return None
        return None

    patched_doc = pipe(
        input_schema,
        apply_transformations(
            fill_empty_node,
            # _limit_depth(),
            add_nulls,
            remove_excess_keys()
        ),
        print
    )

    log("---------")
    log("\t", "\n\t".join(missing_paths))
    log()

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
