import pandas as pd
import requests
import time
import os
import json
from urllib.parse import quote
import xml.etree.ElementTree as ET

def get_semantic_scholar_attention(taxon_name):
    """
    Query Semantic Scholar API for the taxon name and return the number of results.
    """
    try:
        # URL encode the taxon name
        encoded_taxon = quote(taxon_name)
        url = f"https://api.semanticscholar.org/graph/v1/paper/search?query={encoded_taxon}&limit=1&fields=total"
        
        headers = {
            "User-Agent": "Research Project - Taxon Analysis"
        }
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            # Return the total number of results
            return data.get('total', 0)
        else:
            print(f"Error with Semantic Scholar API for {taxon_name}: {response.status_code}")
            return 0
    except Exception as e:
        print(f"Exception while querying Semantic Scholar for {taxon_name}: {e}")
        return 0

def get_pubmed_attention(taxon_name):
    """
    Query PubMed API for the taxon name and return the number of results.
    """
    try:
        # URL encode the taxon name
        encoded_taxon = quote(taxon_name)
        
        # Use NCBI's E-utilities API (esearch)
        url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term={encoded_taxon}&retmode=json"
        
        headers = {
            "User-Agent": "Research Project - Taxon Analysis"
        }
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            # Return the total number of results from PubMed
            return int(data.get('esearchresult', {}).get('count', 0))
        else:
            print(f"Error with PubMed API for {taxon_name}: {response.status_code, response.text}")
            return 0
    except Exception as e:
        print(f"Exception while querying PubMed for {taxon_name}: {e}")
        return 0

def get_wikidata_year(taxon_name):
    """
    Query Wikidata for the taxon name and return the year of first description.
    Uses taxon-specific properties for better results.
    """
    try:
        # Improve the SPARQL query to better identify taxonomic entities
        query = f"""
        SELECT ?item ?itemLabel ?inception WHERE {{
          # Try multiple approaches to find the taxon
          {{
            # Search by scientific name (P225)
            ?item wdt:P225 "{taxon_name}".
          }} UNION {{
            # Search by taxonomic name
            ?item rdfs:label "{taxon_name}"@en.
            ?item wdt:P31/wdt:P279* wd:Q16521. # instance of taxon or subclass
          }} UNION {{
            # Search by label for any taxonomic entity
            ?item rdfs:label "{taxon_name}"@en.
            ?item wdt:P105 ?taxonRank. # has taxon rank
          }}
          
          # Get the inception/description date if available
          OPTIONAL {{ 
            ?item wdt:P571 ?inception. # inception/description date
          }}
          
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
        }}
        LIMIT 1
        """
        
        url = "https://query.wikidata.org/sparql"
        headers = {
            "User-Agent": "Research Project - Taxon Analysis",
            "Accept": "application/json"
        }
        
        response = requests.get(
            url, 
            headers=headers, 
            params={"query": query, "format": "json"}
        )
        
        if response.status_code == 200:
            data = response.json()
            results = data.get('results', {}).get('bindings', [])
            
            if results:
                # Debug output to see what's being returned
                print(f"  Wikidata found: {len(results)} results")
                
                if 'inception' in results[0]:
                    # Extract the year from the date value
                    date_value = results[0]['inception']['value']
                    # Date format could be YYYY or YYYY-MM-DD
                    year = date_value.split('-')[0] if '-' in date_value else date_value
                    return int(year) if year.isdigit() else None
                else:
                    print(f"  No inception date found for {taxon_name}")
                    return None
            else:
                print(f"  No Wikidata entity found for {taxon_name}")
                return None
        else:
            print(f"Error with Wikidata API for {taxon_name}: {response.status_code}")
            if response.status_code == 429:
                print("  Rate limit exceeded, waiting 60 seconds")
                time.sleep(60)  # Wait longer if rate limited
            return None
    except Exception as e:
        print(f"Exception while querying Wikidata for {taxon_name}: {e}")
        return None

def enrich_taxon_data(input_file, output_file, batch_size=100):
    """
    Process the taxon data file and add attention from multiple sources and year of first description.
    
    Args:
        input_file: Path to the input CSV with taxon data
        output_file: Path to save the enriched data
        batch_size: Number of records to process before saving a checkpoint
    """
    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(output_file)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Create checkpoint directory
    checkpoint_dir = os.path.join(output_dir, 'checkpoints')
    if not os.path.exists(checkpoint_dir):
        os.makedirs(checkpoint_dir)
    
    # Load the taxon data
    print(f"Loading taxon data from {input_file}")
    taxon_data = pd.read_csv(input_file)
    
    # Initialize new columns
    if 'attention_ss' not in taxon_data.columns:
        taxon_data['attention_ss'] = None
    if 'attention_pm' not in taxon_data.columns:
        taxon_data['attention_pm'] = None
    if 'year_ofd' not in taxon_data.columns:
        taxon_data['year_ofd'] = None
    
    # Check for existing checkpoint
    latest_checkpoint = None
    checkpoint_files = [f for f in os.listdir(checkpoint_dir) if f.startswith('checkpoint_') and f.endswith('.csv')]
    if checkpoint_files:
        latest_checkpoint = os.path.join(checkpoint_dir, sorted(checkpoint_files)[-1])
        print(f"Found checkpoint: {latest_checkpoint}")
        taxon_data = pd.read_csv(latest_checkpoint)
    
    total_rows = len(taxon_data)
    print(f"Processing {total_rows} taxa")
    
    # Process each taxon
    start_idx = 0
    for idx in range(total_rows):
        # Skip already processed rows
        if not pd.isna(taxon_data.at[idx, 'attention_ss']) and not pd.isna(taxon_data.at[idx, 'attention_pm']) and not pd.isna(taxon_data.at[idx, 'year_ofd']):
            start_idx = idx + 1
            continue
        
        taxon_name = taxon_data.at[idx, 'taxon_name']
        print(f"Processing {idx+1}/{total_rows}: {taxon_name}")
        
        # Get attention from Semantic Scholar
        if pd.isna(taxon_data.at[idx, 'attention_ss']):
            attention_ss = get_semantic_scholar_attention(taxon_name)
            taxon_data.at[idx, 'attention_ss'] = attention_ss
            print(f"  Semantic Scholar attention: {attention_ss}")
            time.sleep(0.5)  # Brief pause between API calls
        
        # Get attention from PubMed
        if pd.isna(taxon_data.at[idx, 'attention_pm']):
            attention_pm = get_pubmed_attention(taxon_name)
            taxon_data.at[idx, 'attention_pm'] = attention_pm
            print(f"  PubMed attention: {attention_pm}")
            time.sleep(0.5)  # Brief pause between API calls
        
        # Get year of first description from Wikidata
        if pd.isna(taxon_data.at[idx, 'year_ofd']):
            year = get_wikidata_year(taxon_name)
            taxon_data.at[idx, 'year_ofd'] = year
            print(f"  Year of first description: {year}")
        
        # Create checkpoint after processing batch_size records
        if (idx + 1) % batch_size == 0 or idx == total_rows - 1:
            checkpoint_file = os.path.join(checkpoint_dir, f"checkpoint_{idx+1}.csv")
            taxon_data.to_csv(checkpoint_file, index=False)
            print(f"Saved checkpoint to {checkpoint_file}")
        
        # Be gentle with the APIs - pause between taxon processing
        time.sleep(1)
    
    # Save final result
    taxon_data.to_csv(output_file, index=False)
    print(f"Enrichment complete. Results saved to {output_file}")

def main():
    # Configuration
    input_file = '/app/data/final_01_degree.csv'
    output_file = '/app/exports/final_01_attention.csv'
    batch_size = 50  # Create checkpoint after processing this many records
    
    # Process
    enrich_taxon_data(input_file, output_file, batch_size)

if __name__ == "__main__":
    main()