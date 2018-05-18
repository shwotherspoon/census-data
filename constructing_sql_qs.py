import sqlite3
import numpy as np 
from pie_charts import draw_chart


def get_data(input_args):
    '''
    input args looks like this ! 
    [["housing",["buy",10000], 5],["family","no_family",4],["location","N",3],["education","high","2"],["median_household_income","high",1],["walk_score","high",1]]
    '''

    db = sqlite3.connect("cbsa.db")
    c = db.cursor()

    criteria_list = [x[0] for x in input_args]

    unemployment_weight = str(-0.35)
    crime_weight = str(-0.35)
    walk_score_weight = str(0.2)
    education_weight = str(0.1)

    formula = "education_level*"+education_weight+'+'+"walk_score*"+walk_score_weight+'+'+"unemployment*"+unemployment_weight+'+'+"crime_rate*"+crime_weight

    '''
    cbsa_name, region, unemployment*0.25 - crime_rate*0.25 + education_level*0.25 + walk_score*0.25 + (CASE WHEN region = 'northeast' THEN 0.25 ELSE 0 END) AS score FROM master ORDER BY score DESC LIMIT 5;
    '''
    job = False 
    if 'jobs' in criteria_list:
        job = True 

    input_list = []

    for criteria in criteria_list:
        index = criteria_list.index(criteria)
        tupe = input_args[index]
        if criteria!= 'walk_score':
            print(criteria)
            val = tupe[1] 
            rank = tupe[2] 
        if criteria == 'housing':
            formula += '+'
            price = val[1]
            if val[0] == 'buy':
                formula += "(CASE WHEN housing_cost <= ? THEN ? ELSE 0 END)"
            else:
                formula += '(CASE WHEN median_gross_rent <= ? THEN ? ELSE 0 END)'
            input_list.append(price)
            input_list.append(str(rank))
            print('appended val and rank as {} and {} for {}'.format(price,rank,criteria))
        if criteria == 'family_households':
            formula += '+'
            if val == 'high':
                formula += "(CASE WHEN family_level = ? THEN ? ELSE 0 END)"
            elif val == 'med':
                formula +=  "(CASE WHEN family_level = ? THEN ? ELSE 0 END)"
            else:
                formula += "(CASE WHEN family_level = ? THEN ? ELSE 0 END)"
            input_list.append(val)
            input_list.append(str(rank))
            print('appended val and rank as {} and {} for {}'.format(val,rank,criteria))
        if criteria == 'location':
            formula += '+'
            if len(val) == 1:
                formula += "(CASE WHEN region = ? THEN ? ELSE 0 END)"
            else:
                formula += "(CASE WHEN state = ? THEN ? ELSE 0 END)"
            input_list.append(val)
            input_list.append(str(rank))
            print('appended val and rank as {} and {} for {}'.format(val,rank,criteria))
        if criteria == 'education':
            formula += '+'
            formula += "(CASE WHEN education_level = ? THEN ? ELSE 0 END)"
            input_list.append(val)
            input_list.append(str(rank))
            print('appended val and rank as {} and {} for {}'.format(val,rank,criteria))
        if criteria == 'median_household_income':
            formula += '+'
            formula += "(CASE WHEN income_level = ? THEN ? ELSE 0 END)"
            input_list.append(val)
            input_list.append(str(rank))
            print('appended val and rank as {} and {} for {}'.format(val,rank,criteria))
        if criteria == 'size':
            formula += '+'
            formula += "(CASE WHEN city_size = ? THEN ? ELSE 0 END)"
            input_list.append(val)
            input_list.append(str(rank))
            print('appended val and rank as {} and {} for {}'.format(val,rank,criteria))
        if criteria == 'walk_score':
            rank = tupe[1]
            formula += '+'
            formula += "(CASE WHEN walk_score = ? THEN ? ELSE 0 END)"
            input_list.append('high')
            input_list.append(str(rank))
            print('appended val and rank as high and {} for {}'.format(rank,criteria))

    temp_table = "CREATE temp TABLE tt as SELECT image_filename,cbsa_name,city_size,median_age,income_level,education_level,male,female,white,black,american_indian,asian,pacific_islander,other_race,two_or_more_races,hispanic_or_latino,unemployment,crime_rate,housing_cost,walk_score,region,"+formula+" as total from master group by cbsa_name;"  
    print(temp_table)

    if input_list == []:
        c.execute(temp_table)
    else:
        print(input_list)
        c.execute(temp_table,input_list)

    sql_query = "SELECT s.image_filename,s.cbsa_name,s.city_size,s.median_age,s.income_level,s.education_level,s.male,s.female,s.white,s.black,s.american_indian,s.asian,s.pacific_islander,s.other_race,s.two_or_more_races,s.hispanic_or_latino,s.unemployment,s.crime_rate,s.housing_cost,s.walk_score,s.region,s.total,(SELECT COUNT(*)+1 FROM tt AS r WHERE r.total > s.total) AS rank FROM tt AS s ORDER BY rank LIMIT 10;"
    results = c.execute(sql_query)
    results = results.fetchall()
    
    rv = []
    for result in results:
        result = list(result)
        sub_list = []
        for i in range(22):
            sub_list.append(result[i])
        rv.append(sub_list)

    if job:
        index = criteria_list.index('jobs')
        key_words = input_list[index][1]
        rank = criteria_list[index][2]
        rv = rank_by_jobs(results,key_words,rank)

    else: 
        rv = rv[:5]

    return rv 


def create_pie_chart_files(top_five,pie_charts_folder=None):
    '''
    Given a list of tuples for the top five CBSAs, generate the gender and 
    race pie charts for each CBSA.
    '''
    pie_folder = 'ranker/pie_charts/'

    counter = 1
    for cbsa in top_five:
        cbsa_name = cbsa[1]
        gender_sizes = [cbsa[6]*100, cbsa[7]*100]
        race_sizes = [('White', cbsa[8]*100), ('Black', cbsa[9]*100), 
                        ('American Indian', cbsa[10]*100), 
                        ('Asian', cbsa[11]*100),
                        ('Pacific Islander', cbsa[12]*100), 
                        ('Two or more races', cbsa[14]*100),
                        ('Other race', cbsa[13]*100)]
        percent_hispanic = cbsa[15] * 100
        gender_file = pie_charts_folder + str(counter) + "_" + "gender"
        race_file = pie_charts_folder + str(counter) + "_" + "race"

        draw_chart('gender', gender_sizes, cbsa_name, hispanic=None, filename=gender_file)
        draw_chart('race', race_sizes, cbsa_name, hispanic=percent_hispanic, filename=race_file)
        counter += 1
