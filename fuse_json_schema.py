from __future__ import annotations
from typing_extensions import NotRequired, TypedDict


class JsonSchema(TypedDict):
    type: list[str] | str
    properties: NotRequired[dict[str, 'JsonSchema']]
    items: NotRequired['JsonSchema']
    format: NotRequired[str]


null_schema: JsonSchema = {"type": "null"}


def zip_dicts(dict_1, dict_2, empty=None):
    all_keys = set(dict_1).union(set(dict_2))
    for key in all_keys:
        yield key, dict_1.get(key, empty), dict_2.get(key, empty)


def fuse_json_schemas(schema_a: JsonSchema = null_schema, schema_b: JsonSchema = null_schema) -> JsonSchema:
    def union_types(type_a: list | str, type_b: list | str) -> list:
        type_a_set = {type_a} if isinstance(type_a, str) else set(type_a)
        type_b_set = {type_b} if isinstance(type_b, str) else set(type_b)
        return list(type_a_set | type_b_set)

    fused_schema: JsonSchema = {"type": union_types(schema_a.get("type", []), schema_b.get("type", []))}

    all_keys = set(schema_a).union(set(schema_b))

    for key in all_keys:
        source_schemas = [sch for sch in (schema_a, schema_b) if key in sch]

        if key == "type":
            continue

        if key == "properties":
            fused_schema["properties"] = dict()
            for prop_key, val_a, val_b in zip_dicts(schema_a.get("properties", {}),
                                                    schema_b.get("properties", {}),
                                                    {}):
                fused_schema["properties"][prop_key] = fuse_json_schemas(val_a, val_b)

        elif key == "items":
            fused_schema["items"] = fuse_json_schemas(*(sch["items"] for sch in source_schemas))

        elif key == "format":
            if len(source_schemas) > 1:
                assert schema_a["format"] == schema_b["format"], (f"irreconcilable formats: "
                                                                  f"{schema_a['format'], schema_b['format']}")
            fused_schema["format"] = schema_a["format"]

        else:
            if len(source_schemas) > 1 and schema_a[key] != schema_b: # type: ignore
                raise ValueError(f"un-handled {key=}")
            fused_schema[key] = schema_a[key]  # type: ignore

    return fused_schema

