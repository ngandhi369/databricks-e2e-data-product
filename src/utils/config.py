import os

env = os.getenv("ENV", "dev")  # default = dev

catalog = os.getenv("CATALOG")
schema = os.getenv("SCHEMA")

volume_path = os.getenv("volume_path")


def get_table(name):
    return f"{catalog}.{schema}.{name}"

# orders.csv -> orders_dev.csv or orders_prod.csv based on environment variable. 
def get_file_path(file_name):
    name, ext = os.path.splitext(file_name)
    return f"{volume_path}/{name}_{env}{ext}"
