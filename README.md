# Pull Airtable data to CSV

1. First, get an [Airtable API key](https://support.airtable.com/hc/en-us/articles/219046777-How-do-I-get-my-API-key-).

2. `git clone` this repo.

3. Open terminal. `cd` into the repo.

4. Run this code 

`python airtable_to_json.py API_KEY 'OUTPUTFILENAME.json'`

Substitute `API_KEY` and `OUTPUTFILENAME` for your own API key and output file
name.

5. Run the below, substituting as necessary.

`python json_to_csv.py 'OUTPUTFILENAME.json' 'OUTPUTFILENAME.csv'`

6. Upload the CSV file to Carto. Save the new database as `airtable` (remember to 
change the old airtable database name). Change the privacy to "public". 
Navigate to the website to check that all the categories are working fine.

P.s. Just in case, I always rename the old database
as `airtable_old` to be able to revert back quickly in case something is wrong 
with the new file.

