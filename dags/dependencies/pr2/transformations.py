'''Module to make POST API calls to endpoints in pr2-transformations.'''

from dependencies.pr2 import utils

def fix_loop_variables(mapping) -> None:
    '''Call endpoint to fix loop variables.'''
    utils.logger.info(f"Fixing loop variables in {mapping['source']} to produce {mapping['destination']}")
    utils.make_api_call(endpoint="fix_loop_variables", json_data=mapping)

def merge_table_versions(mapping) -> None:
    '''Call endpoint to merge table versions.'''
    utils.logger.info(f"Merging {mapping['source']} to {mapping['destination']}")
    utils.make_api_call(endpoint="merge_table_versions", json_data=mapping)