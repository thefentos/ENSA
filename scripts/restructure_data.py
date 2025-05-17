import pandas as pd

globi = pd.read_csv('../data/interactions.csv')


species = pd.concat([pd.DataFrame({'taxon_name': globi['source_taxon_name']}), pd.DataFrame({'taxon_name': globi['target_taxon_name']})], ignore_index=True)['taxon_name']
restructured = pd.DataFrame({'taxon_name': species})
restructured = restructured.value_counts().reset_index()
restructured.columns = ['taxon_name', 'degree']
restructured.to_csv('../exports/final_01_degree.csv', index=False)