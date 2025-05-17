import pandas as pd
import os
import gc

def split_large_csv(input_file, temp_dir, chunk_size=1000000):
    """
    Split a large CSV file into smaller temporary chunks for processing.
    
    Args:
        input_file: Path to the input CSV file
        temp_dir: Directory to save temporary chunk files
        chunk_size: Number of rows per chunk
    """
    # Create temp directory if it doesn't exist
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
    
    # Get the base filename without extension
    base_name = os.path.basename(input_file).split('.')[0]
    
    print(f"Splitting {input_file} into chunks of {chunk_size} rows...")
    
    # Read and write in chunks
    chunk_paths = []
    for i, chunk in enumerate(pd.read_csv(input_file, chunksize=chunk_size)):
        chunk_path = os.path.join(temp_dir, f"{base_name}_chunk_{i}.csv")
        chunk.to_csv(chunk_path, index=False)
        chunk_paths.append(chunk_path)
        print(f"Created chunk {i+1}: {chunk_path}")
        
        # Free memory
        del chunk
        gc.collect()
    
    return chunk_paths

def process_chunks(chunk_paths, output_file):
    """
    Process each chunk to create a consolidated degree count.
    
    Args:
        chunk_paths: List of paths to chunk files
        output_file: Path to save the final output
    """
    print("Processing chunks and counting species occurrences...")
    
    # Initialize empty DataFrame for all species
    all_species = pd.DataFrame(columns=['taxon_name'])
    
    # Process each chunk
    for i, chunk_path in enumerate(chunk_paths):
        print(f"Processing chunk {i+1}/{len(chunk_paths)}")
        
        # Read chunk
        chunk = pd.read_csv(chunk_path)
        
        # Extract species from source and target columns and combine
        species = pd.concat([
            pd.DataFrame({'taxon_name': chunk['sourceTaxonName']}),
            pd.DataFrame({'taxon_name': chunk['targetTaxonName']})
        ], ignore_index=True)
        
        # Append to all_species
        all_species = pd.concat([all_species, species], ignore_index=True)
        
        # Free memory
        del chunk
        del species
        gc.collect()
    
    print("Counting occurrences...")
    
    # Count occurrences (value_counts) exactly as in your original code
    restructured = all_species.value_counts().reset_index()
    restructured.columns = ['taxon_name', 'degree']
    
    # Save result
    output_dir = os.path.dirname(output_file)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    restructured.to_csv(output_file, index=False)
    print(f"Results saved to {output_file}")
    
    return restructured

def cleanup_temp_files(chunk_paths):
    """Remove temporary chunk files."""
    print("Cleaning up temporary files...")
    for path in chunk_paths:
        if os.path.exists(path):
            os.remove(path)
    print("Cleanup complete.")

def main():
    # Configuration
    input_file = '/app/data/interactions.csv'  # Your 30GB file
    temp_dir = '/app/temp/'
    output_file = '/app/exports/final_01_degree.csv'
    chunk_size = 5000  # Adjust based on available memory
    
    # Process
    # chunk_paths = split_large_csv(input_file, temp_dir, chunk_size)
    chunk_paths = []
    for i in os.listdir('/app/temp/'):
        chunk_paths.append(f'/app/temp/{i}') 
    process_chunks(chunk_paths, output_file)
    cleanup_temp_files(chunk_paths)

if __name__ == "__main__":
    main()