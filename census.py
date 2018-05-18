# CS122-Win-17

import bs4
import urllib3
import csv
import sqlite3


def crawl_census(variable_codes=None, cbsa_codes=None, filename='census.csv'):
	'''
	Crawls 2015 American Community Survey

	Inputs:
		variable_codes: a list of strings, each string is a code for a
			census variable
		cbsa_codes: a list of strings, each string is a code for a 
			CBSA/MSA
        filename: (str) optional filename to save results to as a CSV

    Returns:
        If filename provided, creates a file with that filename that 
        contains data crawled from the census. If no filename provided,
        returns a list of lists containing data crawled from the census.
	'''
	# Get CBSA codes
	if not cbsa_codes:
		cbsa_codes = get_cbsa_codes_list()
	cbsa_str = ",".join(cbsa_codes)

	# Get variable codes
	if not variable_codes:
		variable_codes = get_variable_codes_list()
	variables_str = ",".join(variable_codes)

	pm = urllib3.PoolManager()
	key = None #get key from Census
	query_url = "http://api.census.gov/data/2015/acs5?get={}&for=metropolitan+statistical+area/micropolitan+statistical+area:{}&key={}}".format(variables_str, cbsa_str, key)
	html = pm.urlopen(url=query_url, method="GET").data

	soup = bs4.BeautifulSoup(html, "lxml")
	string = soup.get_text("|", strip=True)
	str_split = string.split(",\n")
	str_split = str_split[1:] # Remove header row

	values = []
	for row in str_split:
		no_brackets = row.strip('[]')
		row_vals = no_brackets.split(",")
		# Convert row values to appropriate types (float, int, str)
		new_row_vals = []
		for i, v in enumerate(row_vals):
			v = v.strip('"')
			if i == len(row_vals)-1:
				new_val = int(v)
			elif v != "null":
				new_val = float(v)
			else:
				new_val = v
			new_row_vals.append(new_val)
		values.append(new_row_vals)
	if filename:
		with open(filename, "wt") as f:
			writer = csv.writer(f)
			writer.writerows(values)
	else:
		return values


def get_cbsa_codes_list():
    '''
    Returns a list of CBSA codes from the file 'cbsa_zips.csv'.
    '''
    cbsa_codes = []
    with open('cbsa_data/cbsa_zips.csv', 'r') as f:
        reader = csv.reader(f) 
        for row in reader:     
            cbsa_code = row[3]
            if cbsa_code not in cbsa_codes:
                cbsa_codes.append(cbsa_code)
    f.close()
    return cbsa_codes


def get_variable_codes_list(filename='census_variables.csv'):
    '''
    Returns a list of census variable codes from the spcified
    file (default='census_variables.csv').
    '''
    variable_codes = []
    with open('census_variables.csv', 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            variable_code = row[1]
            variable_codes.append(variable_code)
    f.close()
    return variable_codes
