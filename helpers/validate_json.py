from pathlib import Path
import jsonschema
import argparse
import json

VALIDATOR_VERSIONS: dict[tuple[str, str], type[object]] = {
    ("v201909", "Draft201909Validator"): jsonschema.Draft201909Validator,
    ("v202012", "Draft202012Validator"): jsonschema.Draft202012Validator,
    ("v3", "Draft3Validator"): jsonschema.Draft3Validator,
    ("v4", "Draft4Validator"): jsonschema.Draft4Validator,
    ("v6", "Draft6Validator"): jsonschema.Draft6Validator,
    ("v7", "Draft7Validator"): jsonschema.Draft7Validator,
}


def get_draft(draft_name: str) -> type[jsonschema.Draft7Validator]:
    return [
        draft for names, draft in VALIDATOR_VERSIONS.items() if draft_name in names
    ][0]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("SCHEMA")
    parser.add_argument("DOCUMENT")
    parser.add_argument(
        "--draft",
        # select by long or short name
        choices=[
            *(v[0] for v in VALIDATOR_VERSIONS),
            *(v[1] for v in VALIDATOR_VERSIONS),
        ],
        default="v7",
        help="long or short name of schema draft version",
    )

    args = parser.parse_args(argv)

    schema = json.loads(Path(args.SCHEMA).read_text())
    document = json.loads(Path(args.DOCUMENT).read_text())

    validator = get_draft(args.draft)(schema, format_checker=jsonschema.FormatChecker())

    validator.validate(document)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
