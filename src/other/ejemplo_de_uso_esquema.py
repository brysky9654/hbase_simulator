import pyarrow as pa
import pyarrow.parquet as pq

# Define the schema
schema = pa.schema([
    ('row_key', pa.string()),
    ('column_family1', pa.struct([
        ('column_qualifier1', pa.list_(
            pa.struct([
                ('timestamp', pa.int64()),
                ('value', pa.string())
            ])
        )),
        ('column_qualifier2', pa.list_(
            pa.struct([
                ('timestamp', pa.int64()),
                ('value', pa.string())
            ])
        )),
    ])),
    ('column_family2', pa.struct([
        ('column_qualifier1', pa.list_(
            pa.struct([
                ('timestamp', pa.int64()),
                ('value', pa.string())
            ])
        )),
        ('column_qualifier2', pa.list_(
            pa.struct([
                ('timestamp', pa.int64()),
                ('value', pa.string())
            ])
        )),
    ]))
])

# Create sample data
data = [
    ('row1', {
        'column_family1': {
            'column_qualifier1': [{'timestamp': 1, 'value': 'cell value 1'}, {'timestamp': 2, 'value': 'cell value 2'}],
            'column_qualifier2': [{'timestamp': 1, 'value': 'cell value 3'}, {'timestamp': 2, 'value': 'cell value 4'}],
        },
        'column_family2': {
            'column_qualifier1': [{'timestamp': 1, 'value': 'cell value 5'}, {'timestamp': 2, 'value': 'cell value 6'}],
            'column_qualifier2': [{'timestamp': 1, 'value': 'cell value 7'}, {'timestamp': 2, 'value': 'cell value 8'}],
        }
    })
]

# Convert data to PyArrow Table
def convert_to_table(data, schema):
    rows = []
    for row in data:
        row_key, families = row
        row_data = [
            row_key,
            families['column_family1']['column_qualifier1'],
            families['column_family1']['column_qualifier2'],
            families['column_family2']['column_qualifier1'],
            families['column_family2']['column_qualifier2'],
        ]
        rows.append(row_data)
    return pa.Table.from_pylist(rows, schema=schema)


table = convert_to_table(data, schema)

# Write the table to a Parquet file
pq.write_table(table, 'output.parquet')

# Leer la tabla desde un archivo Parquet
table = pq.read_table('output.parquet')
print(table)



