'''Module to make POST API calls to endpoints in pr2-transformations.'''

from dependencies.pr2 import utils

def clean_columns(mapping) -> None:
    '''Call endpoint to fix loop variables.'''
    utils.logger.info(f"Cleaning columns in {mapping['source']} to produce {mapping['destination']}")
    utils.make_api_call(endpoint="clean_columns", json_data=mapping)

def clean_rows(mapping) -> None:
    '''Call endpoint to fix loop variables.'''
    utils.logger.info(f"Cleaning rows in {mapping['source']} to produce {mapping['destination']}")
    utils.make_api_call(endpoint="clean_rows", json_data=mapping)

def merge_table_versions(mapping) -> None:
    '''Call endpoint to merge table versions.'''
    utils.logger.info(f"Merging {mapping['source']} to {mapping['destination']}")
    utils.make_api_call(endpoint="merge_table_versions", json_data=mapping)