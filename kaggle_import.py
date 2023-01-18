import csv

from datetime import datetime

import psycopg2

username = 'khapusova_diana'
password = 'lask3w0984resh'
database = 'postgres'
host = 'localhost'
port = '5433'

INPUT_FILE = 'deaths.csv'

def find_in_tupple(tup_arr, el, pos_to_find, pos_to_return):
    for elem in tup_arr:
        if elem[pos_to_find] == el:
            return elem[pos_to_return]

def find_place_id(places, country_id, state_id):
    for place in places:
        if place[1] == country_id and place[2] == state_id:
            return place[0]

def insert_data(values, insert_query, select_query, cur, text=""):
    for value in values:
        cur.execute(insert_query, value)
    cur.execute(select_query)
    print(f"\n{text}")
    for row in cur:
        print(row)

def change_to_datetime(arr):
    result = []
    for tup in arr:
        datetime_str = tup[1]
        datetime_object = datetime.strptime(datetime_str, '%m/%d/20%y')
        result.append((tup[0], datetime_object))
    return result

query_1 = '''
DELETE FROM country;
'''
query_2 = '''
INSERT INTO country (country_id, country) VALUES (%s, %s)
'''
query_3 = '''
SELECT * FROM country limit 10
'''
query_4 = '''
DELETE FROM state;
'''
query_5 = '''
INSERT INTO state (state_id, state) VALUES (%s, %s)
'''
query_6 = '''
SELECT * FROM state limit 10
'''
query_7 = '''
DELETE FROM place;
'''
query_8 = '''
INSERT INTO place (place_id, country_id, state_id) VALUES (%s, %s, %s)
'''
query_9 = '''
SELECT * FROM place limit 10
'''
query_10 = '''
DELETE FROM time;
'''
query_11 = '''
INSERT INTO time (time_id, date) VALUES (%s, %s)
'''
query_12 = '''
SELECT * FROM time limit 10
'''
query_13 = '''
DELETE FROM reason;
'''
query_14 = '''
INSERT INTO reason (reason_id, reason) VALUES (%s, %s)
'''
query_15 = '''
SELECT * FROM reason limit 10
'''
query_16 = '''
DELETE FROM death;
'''
query_17 = '''
INSERT INTO death (death_id, place_id, time_id, reason_id, number_of_deaths, is_driver_dead, is_occupant_dead) VALUES (%s, %s, %s, %s, %s, %s, %s)
'''
query_18 = '''
SELECT * FROM death limit 10
'''


conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)

with conn:
    cur = conn.cursor()

    cur.execute(query_16)
    cur.execute(query_13)
    cur.execute(query_10)
    cur.execute(query_7)
    cur.execute(query_4)
    cur.execute(query_1)

    input_file = csv.DictReader(open(INPUT_FILE))

    countries, states, dates, reasons = [], ['-'], [], []
    for idx, row in enumerate(input_file):
        country = row[' Country '].strip()
        state = row[' State '].strip()
        reason = row[' Description '].strip()
        date = row['Date'].strip()

        if country in countries:
            pass
        else:
            countries.append(country)

        if state in states or len(state) == 0:
            pass
        else:
            states.append(state)

        if reason in reasons:
            pass
        else:
            reasons.append(reason)

        if date in dates:
            pass
        else:
            dates.append(date)

    countries = [(i, countries[i]) for i in range(len(countries))]
    states = [(i, states[i]) for i in range(len(states))]
    reasons = [(i, reasons[i]) for i in range(len(reasons))]
    dates = [(i, dates[i]) for i in range(len(dates))]

    places, deaths = [], []
    input_file = csv.DictReader(open(INPUT_FILE))

    for idx, row in enumerate(input_file):
        death_id = int(row['Case #'])
        number_of_deaths = int(row[' Deaths '])
        is_driver_dead = row[' Tesla driver '] != ' - '
        is_occupant_dead = row[' Tesla occupant '] != ' - '
        country = row[' Country '].strip()
        state = row[' State '].strip()
        reason = row[' Description '].strip()
        reason_id = find_in_tupple(reasons, reason, 1, 0)
        time_id = find_in_tupple(dates, date, 1, 0)
        country_id = find_in_tupple(countries, country, 1, 0)
        state_id = find_in_tupple(states, state, 1, 0)
        date = row['Date'].strip()
        place_id = find_place_id(places, country_id, state_id or 0)
        if not place_id:
            place_id = len(places) + 1
            places.append((place_id, country_id, state_id or 0))
        values = (death_id, place_id, time_id, reason_id, number_of_deaths, is_driver_dead, is_occupant_dead)
        deaths.append(values)

    insert_data(countries, query_2, query_3, cur, "Countries")
    insert_data(states, query_5, query_6, cur, "States")
    insert_data(places, query_8, query_9, cur, "Places")
    insert_data(change_to_datetime(dates), query_11, query_12, cur, "Countries")
    insert_data(reasons, query_14, query_15, cur, "Reasons")
    insert_data(deaths, query_17, query_18, cur, "Deaths")
    conn.commit()