import argparse

def get_config():
    parser = argparse.ArgumentParser()

    parser.add_argument("--catalog")
    parser.add_argument("--schema")
    parser.add_argument("--volume_path")

    args, _ = parser.parse_known_args()

    return {
        "catalog": args.catalog,
        "schema": args.schema,
        "volume_path": args.volume_path
    }