import pandas as pd
from sqlalchemy import create_engine
import pyarrow as pa
import pyarrow.parquet as pq
from fastavro import writer, parse_schema

# Step 1: Connect to the Database
# Update with your database connection details
server = 'your_server_name'  # e.g., 'localhost' or 'DESKTOP-XXXXXXX'
database = 'SampleDB'
username = 'your_username'
password = 'your_password'

# Connection string for SQLAlchemy
connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server'

engine = create_engine(connection_string)

# Step 2: Query Data using SQL
query = "SELECT * FROM Employees"
df = pd.read_sql(query, engine)

# Step 3: Export to CSV
df.to_csv('data.csv', index=False)

# Step 4: Export to Parquet
table = pa.Table.from_pandas(df)
pq.write_table(table, 'data.parquet')

# Step 5: Export to Avro
# Define Avro schema based on DataFrame schema
def create_avro_schema(df):
    fields = []
    for column, dtype in df.dtypes.items():
        avro_type = "string"
        if pd.api.types.is_integer_dtype(dtype):
            avro_type = "int"
        elif pd.api.types.is_float_dtype(dtype):
            avro_type = "float"
        elif pd.api.types.is_bool_dtype(dtype):
            avro_type = "boolean"
        fields.append({"name": column, "type": avro_type})
    return {
        "type": "record",
        "name": "DataRecord",
        "fields": fields
    }

avro_schema = create_avro_schema(df)
parsed_schema = parse_schema(avro_schema)

# Convert DataFrame to records
records = df.to_dict(orient='records')

# Write records to Avro file
with open('data.avro', 'wb') as out:
    writer(out, parsed_schema, records)

print("Data exported to CSV, Parquet, and Avro formats.")
