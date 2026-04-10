import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

# Read the file paths from parquet_file_paths.txt
with open('parquet_file_paths.txt', 'r') as file:
    lines = file.readlines()
    original_file_path = lines[4].strip()  # The original data file is the fifth line
    new_file_paths = [line.strip() for line in lines[5:]]  # The new files start from the sixth line

# Define paths for output files
split_folder = os.path.join(os.getcwd(), 'split_files')
output_path = r'D:\sales_all_303132_apr10.parquet'

# Ensure the directory for split files exists
os.makedirs(split_folder, exist_ok=True)


# Function to get the union schema of all DataFrames
def get_union_schema(dfs):
    all_columns = set()
    for df in dfs:
        all_columns.update(df.columns)
    return sorted(all_columns)


# Function to unify schema by filling missing columns with NaNs
def unify_schema(df, all_columns):
    for col in all_columns:
        if col not in df.columns:
            df[col] = pd.NA
    return df[all_columns]


# Function to read a Parquet file in chunks
def read_parquet_in_chunks(file_path, chunk_size):
    parquet_file = pq.ParquetFile(file_path)
    for batch in parquet_file.iter_batches(batch_size=chunk_size):
        yield pa.Table.from_batches([batch]).to_pandas()


# Determine the schema from the first chunk of the original file
initial_chunk = next(read_parquet_in_chunks(original_file_path, chunk_size=10000))
all_columns = get_union_schema([initial_chunk])
unified_schema = pa.Schema.from_pandas(initial_chunk[all_columns])


# Function to process DataFrames in chunks and write to Parquet with a progress bar
def process_and_write_in_chunks(file_paths, chunk_size, output_path, schema):
    writer = None
    total_files = len(file_paths)

    pbar_files = tqdm(total=total_files, desc="Merging files", unit='file')

    for file_path in file_paths:
        processed_rows = 0
        pbar_rows = tqdm(desc=f"Processing {os.path.basename(file_path)}", unit='rows')

        for chunk in read_parquet_in_chunks(file_path, chunk_size):
            chunk = unify_schema(chunk, all_columns)
            processed_rows += len(chunk)
            table = pa.Table.from_pandas(chunk, schema=schema)
            if writer is None:
                writer = pq.ParquetWriter(output_path, table.schema)
            writer.write_table(table)
            pbar_rows.update(len(chunk))
            pbar_files.set_postfix({"Current File": os.path.basename(file_path), "Processed Rows": processed_rows})
            pbar_files.update(0)  # Force update the file progress bar with postfix

        pbar_rows.close()
        pbar_files.update(1)

    pbar_files.close()

    if writer is not None:
        writer.close()


# Merge the original and new files together
process_and_write_in_chunks([original_file_path] + new_file_paths, chunk_size=100000, output_path=output_path,
                            schema=unified_schema)

# Verify the merged Parquet file by reading it in chunks
for chunk in read_parquet_in_chunks(output_path, chunk_size=100000):
    print("First 5 rows of a chunk from the merged dataframe:")
    print(chunk.head())
    break