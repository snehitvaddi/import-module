# Notebook 3 - Read all file paths and pass to next notebook

import pandas as pd
import os

# Define the base path where cluster files are stored
base_path = "/data/cluster_files/summary/20250522906"

print(f"Reading files from: {base_path}")

# Initialize list to store all valid file paths
final_output_paths_to_consider = []

# Check if base path exists
if os.path.exists(base_path):
    # Get all files in the directory
    all_files = os.listdir(base_path)
    
    print(f"Found {len(all_files)} files in directory")
    
    # Filter for JSON files (assuming cluster files are JSON)
    json_files = [f for f in all_files if f.endswith('.json')]
    
    print(f"Found {len(json_files)} JSON files")
    
    # Build full file paths
    for file_name in json_files:
        file_path = os.path.join(base_path, file_name)
        
        # Verify file exists and is not empty
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            final_output_paths_to_consider.append(file_path)
            print(f"Added: {file_name}")
        else:
            print(f"Skipped (empty/missing): {file_name}")
            
else:
    print(f"❌ Path does not exist: {base_path}")

print(f"\n✅ Total files to process: {len(final_output_paths_to_consider)}")
print("Files to be passed to next notebook:")
for i, path in enumerate(final_output_paths_to_consider[:5]):  # Show first 5
    print(f"  {i+1}. {path.split('/')[-1]}")
if len(final_output_paths_to_consider) > 5:
    print(f"  ... and {len(final_output_paths_to_consider) - 5} more files")

# Save the file list path using dbutils.jobs.taskValues.set
dbutils.jobs.taskValues.set(key="nb_3_batches_write_path", value=final_output_paths_to_consider)

print(f"\n🚀 Passed {len(final_output_paths_to_consider)} file paths to next notebook via 'nb_3_batches_write_path'")
