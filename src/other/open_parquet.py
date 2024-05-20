import pyarrow.parquet as pq

# Leer la tabla desde un archivo Parquet
table = pq.read_table('output.parquet')

# Convertir la tabla a un DataFrame de Pandas para una manipulación más fácil
df = table.to_pandas()
print(df)