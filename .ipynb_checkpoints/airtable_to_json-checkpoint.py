import argparse
import requests
import json
import time

BASE_URL = "https://api.airtable.com/v0/appr1xuf08nFiiLTF/{tablename}?maxRecords=10000&pageSize=100&view=Export%20View"
TABLES = [
    'services',
    'locations',
    #'organizations',
    #'contact',
    'phones',
    'address',
    'schedule',
    'service_area',
    'taxonomy',
    'details',
]

def get_json(table_name, results, api_key):
    table_results = {}
    has_next_page = True
    result_json = {}

    while has_next_page:
        url = BASE_URL.format(tablename=table_name)
        if 'offset' in result_json:
            url += "&offset={offset}".format(offset=result_json['offset'])
        headers = {'Authorization': 'Bearer {apikey}'.format(apikey=api_key)}
        resp = requests.get(url=url, headers=headers)
        result_json = json.loads(resp.text)

        records = result_json['records']
        table_results.update({rec['id']: rec for rec in records})
        print("Retrieved a page of {tablename}. Waiting 2 seconds to avoid API limit...".format(tablename=table_name))
        time.sleep(2)

        has_next_page = ('offset' in result_json)

    results[table_name] = table_results

def get_all(api_key):
    results = {}
    for table in TABLES:
        get_json(table, results, api_key)
    return results

def write_to_file(results, output_filename):
    with open(output_filename, 'w') as outfile:
        json.dump(results, outfile)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Pull all tables from BAC Airtable Base as JSON.')
    parser.add_argument('api_key', help='API key from https://airtable.com/account')
    parser.add_argument('output_filename', help='Filename to write the output to.')
    args = parser.parse_args()
    results = get_all(args.api_key)
    write_to_file(results, args.output_filename)
