This repo is meant to help merging sales_all files. 
You are invited to modify this code to adapt to other databases.
You need to 
1. run find_route.py
   it will return a file called parquet_file_paths.txt
2. Modify parquet_file_paths.txt and add your old parquet files in the fifth line. 
3. run merge_all.py
   it will read parquet_file_paths.txt. and merge the old parquet file with the new separated ones. 