import os

dbutils.widgets.text("VOLUME_PATH", "")
dbutils.widgets.text("CATALOG", "")
dbutils.widgets.text("SCHEMA", "")
dbutils.widgets.text("ENV", "dev")

volume_path = dbutils.widgets.get("VOLUME_PATH")
catalog = dbutils.widgets.get("CATALOG")
schema = dbutils.widgets.get("SCHEMA")
env = dbutils.widgets.get("ENV")

print(f"VOLUME_PATH:{volume_path}")
print(f"CATALOG:{catalog}")
print(f"SCHEMA:{schema}")
print(f"ENV:{env}")

def get_table(name):
    return f"{catalog}.{schema}.{name}"

# orders.csv -> orders_dev.csv or orders_prod.csv based on environment variable. 
def get_file_path(file_name):
    name, ext = os.path.splitext(file_name)
    return f"{volume_path}/{name}_{env}{ext}"
