import os

# Function to find all parquet files in the specified directory and subdirectories
def find_parquet_files(root_dir):
    parquet_files = []
    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith('.parquet'):
                parquet_files.append(os.path.join(dirpath, filename))
    return parquet_files

# Define the root directory to start the search
root_dir = os.getcwd()

# Find all parquet files
file_paths = find_parquet_files(root_dir)

# Define the output path for the text file containing the list of parquet files
output_file_path = os.path.join(root_dir, 'parquet_file_paths.txt')

# Save the file paths to the output text file
with open(output_file_path, 'w') as f:
    for file_path in file_paths:
        f.write(file_path + '\n')

print(f"Found {len(file_paths)} parquet files.")
print(f"File paths have been saved to {output_file_path}.")
