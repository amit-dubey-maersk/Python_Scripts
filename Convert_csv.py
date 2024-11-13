import pandas as pd
import os

# Set directory paths
html_folder = '/Users/Amit.Dubey/Downloads/20240827costs_by_voyage/'  
output_folder = '/Users/Amit.Dubey/Downloads/20240827costs_by_voyage_csv'  
output_format = 'csv'  

# Ensure output folder exists
os.makedirs(output_folder, exist_ok=True)

# Process each HTML file
for filename in os.listdir(html_folder):
    if filename.endswith('.html'):
        file_path = os.path.join(html_folder, filename)
        
        # Load HTML file
        tables = pd.read_html(file_path)  # returns a list of dataframes
        
        # Save each table
        for i, table in enumerate(tables):
            output_file = os.path.join(
                output_folder,
                f"{os.path.splitext(filename)[0]}_table{i}.{output_format}"
            )
            
            # Save as CSV or Parquet
            if output_format == 'csv':
                table.to_csv(output_file, index=False)
            elif output_format == 'parquet':
                table.to_parquet(output_file, index=False)
                
print("Conversion complete!")