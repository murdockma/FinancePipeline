import os
import pandas as pd
import numpy as np
import random
import json

from google.cloud import bigquery
import pandas_gbq

def load_configurations(mapping_file='mappings.json', data_paths_file='data_paths.json'):
    """Load mappings and data paths from JSON files"""
    configurations = {}

    with open(mapping_file, 'r') as file:
        configurations['mappings'] = json.load(file)

    with open(data_paths_file, 'r') as file:
        configurations['data_paths'] = json.load(file)
    
    return configurations

def categorize_transaction_reason(reason, category_mapping):
    """Categorize transaction based on reason"""
    for keyword, category in category_mapping.items():
        if keyword in reason:
            return category
    return np.nan

def generate_unique_transaction_id(existing_ids, seed=42):
    """Generate a unique trans ID not in the current set of IDs"""
    random.seed(seed)
    new_id = random.randint(100000, 999999)
    while new_id in existing_ids:
        new_id = random.randint(100000, 999999)
    return new_id

def assign_transaction_ids(transactions_df):
    """Generate and assign unique UUIDs for each trans in the df"""
    transactions_df = transactions_df.copy()
    if 'trans_id' not in transactions_df.columns:
        transactions_df['trans_id'] = None
        
    existing_ids = set(transactions_df['trans_id'].tolist())
    transactions_df['trans_id'] = [generate_unique_transaction_id(existing_ids) for _ in range(len(transactions_df))]
    return transactions_df

def set_transaction_dtypes(transactions_df):
    """Set data types"""
    transactions_df = transactions_df.astype({
        'trans_id': 'str',
        'category': 'str',
        'd_date': 'datetime64[ns]',
        'raw_reason': 'str',
        'amount': 'float'
    })
    return transactions_df

def upload_transactions_to_bigquery(df, project='electric-cortex-289700', database='transactions', table='f_unified_transactions'):
    """Upload df to a BigQuery table"""
    try:
        existing_transactions = pandas_gbq.read_gbq(
            f'SELECT DISTINCT trans_id FROM {project}.{database}.{table}',
            project_id=project
        )
        new_transactions = df[~df['trans_id'].isin(existing_transactions['trans_id'])]
        pandas_gbq.to_gbq(
            new_transactions,
            destination_table=f'{project}.{database}.{table}', 
            project_id=project, 
            if_exists='append'
        )
        print(f'Transactions Added: {len(new_transactions)}')
    except Exception as e:
        print(f"Error uploading transactions: {e}")

def import_bank_transaction_data(file_path, is_credit, column_names):
    """Import bank transaction data from CSV file"""
    df = pd.read_csv(file_path, header=None, names=column_names)
    if is_credit:
        df = df[~df['raw_reason'].str.contains('ONLINE PAYMENT THANK YOU|AUTOMATIC PAYMENT - THANK YOU')]
        df['account'] = 'credit'
    else:
        df['account'] = 'checking'
    return df[['d_date', 'amount', 'raw_reason', 'account']]

def concatenate_transaction_data(file_paths, column_names):
    """Concat trans data from multiple files into a single df"""
    data_frames = [import_bank_transaction_data(file_path, is_credit, column_names) for file_path, is_credit in file_paths.items()]
    return pd.concat(data_frames, ignore_index=True)

def process_transactions(file_paths, column_names, category_mapping):
    """Categorize, dedupe, and apply custom flags to raw transaction data."""
    concatenated_df = concatenate_transaction_data(file_paths, column_names)
    concatenated_df['category'] = concatenated_df['raw_reason'].apply(
        lambda reason: categorize_transaction_reason(reason, category_mapping)
    )
    deduplicated_df = concatenated_df.drop_duplicates(subset=['raw_reason', 'amount']).reset_index(drop=True)
    detailed_df = assign_transaction_ids(deduplicated_df)
    flagged_df = add_custom_transaction_flags(detailed_df)
    flagged_df['transaction_type'] = np.where(flagged_df['amount'] > 0, 'credit', 'debit')
    return set_transaction_dtypes(flagged_df)


def main():
    # Load configs and process transactions
    configs = load_configurations()
    category_mapping, data_paths = configs['mappings'], configs['data_paths']
    column_names = ['d_date', 'amount', 'drop_f', 'drop_z', 'raw_reason']
    transactions_df = process_transactions(
        data_paths,
        column_names,
        category_mapping
    )

    # Upload to BQ
    upload_transactions_to_bigquery(transactions_df)


if __name__ == "__main__":
    main()
