import csv
import os
import sqlite3


def execute_query(query, database='Django/final/cbsa.db'):
    '''
    Executes a given query in the database (default='Django/final/cbsa.db').
    Modifies database.
    '''

    db = sqlite3.connect(database)
    c = db.cursor()
    c.execute(query)
    db.commit()
    db.close()


def import_csv_to_table(csv_filename, insert_query, database='Django/final/cbsa.db'):
    '''
    Imports data from csv file into its corresponding table in the database
    (default='Django/final/cbsa.db'). Modifies database.
    '''
    db = sqlite3.connect(database)
    c = db.cursor()

    f = open(csv_filename)
    csv_reader = csv.reader(f, delimiter=',',\
                            quotechar='"')
    for row in csv_reader:
        db.execute(insert_query, row)
    f.close()

    db.commit()
    db.close()


def make_table(create_table_query, insert_query, csv_filename,
                    database='Django/final/cbsa.db'):
    '''
    Initializes a SQL table in the given database (default='Django/final/cbsa.db') and
    imports data from the specified CSV file into the table. Modifies database.
    '''
    execute_query(create_table_query, database)
    import_csv_to_table(csv_filename, insert_query, database)


### Add individual tables to cbsa.db database ###    

def add_cbsa_zips(csv_filename='data/cbsa_zips.csv', database='Django/final/cbsa.db'):
    '''
    Generates a table for cbsa_zips in the cbsa.db database. Modifies database.
    '''
    create_table_query = "CREATE TABLE cbsa_zips (cbsa_name VARCHAR(30), \
                        zip_code VARCHAR(5), state VARCHAR(2), \
                        cbsa_code integer);"
    insert_query = "INSERT INTO cbsa_zips (cbsa_name, zip_code, state, \
                            cbsa_code) VALUES (?, ?, ?, ?);"
    make_table(create_table_query, insert_query, csv_filename, database)


def add_census(csv_filename='data/census.csv', database='Django/final/cbsa.db'):
    '''
    Generates a table for census in the cbsa.db database. Modifies database.
    '''
    create_table_query = "CREATE TABLE census (total_pop VARCHAR(10), \
                            male VARCHAR(10), \
                            female VARCHAR(10), \
                            median_age VARCHAR(10), \
                            white_alone VARCHAR(10), \
                            black_alone VARCHAR(10), \
                            american_indian_alone VARCHAR(10), \
                            asian_alone VARCHAR(10), \
                            pacific_islander_alone VARCHAR(10), \
                            other_race_alone VARCHAR(10), \
                            two_or_more_races VARCHAR(10), \
                            hispanic_or_latino VARCHAR(10), \
                            total_households VARCHAR(10), \
                            family_households VARCHAR(10), \
                            total_25_years_plus VARCHAR(10), \
                            associates VARCHAR(10), \
                            bachelors VARCHAR(10), \
                            masters VARCHAR(10), \
                            doctorate VARCHAR(10), \
                            median_household_income VARCHAR(10), \
                            civilian_labor_force VARCHAR(10), \
                            unemployed VARCHAR(10), \
                            median_gross_rent VARCHAR(10), \
                            education_level VARCHAR(4), \
                            income_level VARCHAR(4), \
                            city_size VARCHAR(4), \
                            family_level VARCHAR(4), \
                            cbsa_code integer);"
    insert_query = "INSERT INTO census (total_pop, male, female, median_age, \
                            white_alone, black_alone, american_indian_alone, \
                            asian_alone, pacific_islander_alone, other_race_alone, \
                            two_or_more_races, hispanic_or_latino, \
                            total_households, family_households, \
                            total_25_years_plus, associates, bachelors, \
                            masters, doctorate, median_household_income, \
                            civilian_labor_force, unemployed, median_gross_rent, \
                            cbsa_code, education_level, income_level, \
                            city_size, family_level) \
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
    make_table(create_table_query, insert_query, csv_filename, database)


def add_percentages(database='Django/final/cbsa.db'):
    '''
    Generates a table for percentages in the cbsa.db database. Modifies database.
    '''
    create_table_query = "CREATE TABLE percentages AS \
        SELECT cbsa_code, \
        male/total_pop AS 'male', \
        female/total_pop AS 'female', \
        white_alone/total_pop AS 'white', \
        black_alone/total_pop AS 'black', \
        american_indian_alone/total_pop AS 'american_indian', \
        asian_alone/total_pop AS 'asian', \
        pacific_islander_alone/total_pop AS 'pacific_islander', \
        other_race_alone/total_pop AS 'other_race', \
        two_or_more_races/total_pop AS 'two_or_more_races', \
        hispanic_or_latino/total_pop AS 'hispanic_or_latino', \
        family_households/total_households AS 'family_households', \
        (associates + bachelors + masters + doctorate)/total_25_years_plus \
        AS 'degree_holders', \
        unemployed/civilian_labor_force AS 'unemployment' \
        FROM census;"
    execute_query(create_table_query, database)
