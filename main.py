import psycopg2
import matplotlib.pyplot as plt

username = 'khapusova_diana'
password = 'lask3w0984resh'
database = 'postgres'
host = 'localhost'
port = '5433'

query_1 = '''
create view StateDeaths as
SELECT state, SUM(number_of_deaths) FROM death
INNER JOIN place ON death.place_id = place.place_id
INNER JOIN state ON place.state_id = state.state_id
WHERE place.state_id != 0
GROUP BY state;
'''
query_2 = '''
create view CountryDeaths as
SELECT country, SUM(deaths)/(
SELECT sum(deaths) FROM 
(SELECT country, sum(number_of_deaths) as deaths FROM death
INNER JOIN place ON death.place_id = place.place_id
INNER JOIN country ON place.country_id = country.country_id
group by country) AS table1) as death_percentage FROM (SELECT country, sum(number_of_deaths) as deaths FROM death
INNER JOIN place ON death.place_id = place.place_id
INNER JOIN country ON place.country_id = country.country_id
GROUP BY country) AS table2
GROUP BY country;
'''
query_3 = '''
create view YearDeaths as
SELECT EXTRACT(YEAR FROM date) as years, sum(number_of_deaths) as deaths FROM death
INNER JOIN time ON death.time_id = time.time_id
GROUP BY years
ORDER BY years
'''

conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)

x, y = [], []
with conn:
    print("Database opened successfully")

    cur = conn.cursor()
    cur.execute('DROP VIEW IF EXISTS StateDeaths')
    cur.execute(query_1)
    cur.execute('SELECT * FROM StateDeaths')

    for row in cur:
        x.append(row[0][:2])
        y.append(row[1])

    print(x)
    plt.bar(x, y, width=1, alpha=0.8, color='red')
    plt.ylabel('Кількість смертей')
    plt.title('Кількість смертей в штатах США')
    plt.show()

    x.clear()
    y.clear()
    cur.execute('DROP VIEW IF EXISTS CountryDeaths')
    cur.execute(query_2)
    cur.execute('SELECT * FROM CountryDeaths')
    for row in cur:
        x.append(row[0][:12])
        y.append(row[1])
    plt.pie(y, labels=x, shadow=True, autopct='%1.1f%%', startangle=180)
    plt.title('Частка смертей в країнах')
    plt.show()

    x.clear()
    y.clear()
    cur.execute('DROP VIEW IF EXISTS YearDeaths')
    cur.execute(query_3)
    cur.execute('SELECT * FROM YearDeaths')
    for row in cur:
        y.append(row[1])
        x.append(row[0])
    plt.plot(x, y, 'go-')
    plt.ylabel('Кількість смертей')
    plt.xlabel('Рік')
    plt.title('Показники смертей по рокам')
    for x, y in zip(x, y):
        plt.annotate(y, xy=(x, y), xytext=(7, 2), textcoords='offset points')
    plt.show()
