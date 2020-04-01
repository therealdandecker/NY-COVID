#!/usr/bin/env python
# coding: utf-8

#Import libraries 
import requests
import pandas as pd
import bs4
from bs4 import BeautifulSoup
import sqlalchemy
from sqlalchemy import create_engine
#Create SQLite connection
conn = create_engine('sqlite://')

#Scrape data from New York State website
res = requests.get("https://coronavirus.health.ny.gov/county-county-breakdown-positive-cases")
#Parse Data and get it ready for Pandas
soup = BeautifulSoup(res.content,'lxml')
table = soup.find_all('table')[0] 
#Put it in a Pandas Dataframe
NY = pd.read_html(str(table))[0]

#Same thing here
res = requests.get("https://www.newyork-demographics.com/counties_by_population")
soup = BeautifulSoup(res.content,'lxml')
table = soup.find_all('table')[0] 
NYPop = pd.read_html(str(table))[0]

#Pass dataframes to SQL for cleaning
NYPop.to_sql('NYPop',conn, if_exists='replace')
NY.to_sql('NYCov', conn, if_exists='replace')

#Clean in SQL, I think its easier.
NYCovid = pd.read_sql('''
With Pop as (
Select 
case when County in (
'Kings County','Queens County', 'New York County', 'Bronx County','Richmond County')
then 'New York City'
else replace(County,'County','') end as County,
sum(Population) as Population
from NYPop
group by 
case when County in (
'Kings County','Queens County', 'New York County', 'Bronx County','Richmond County')
then 'New York City'
else replace(County,'County','') end
)

Select p.County, p.Population, case when [Positive Cases] is null then 0 else [Positive Cases] end as PositiveCases from Pop p
LEFT JOIN NYCov c on ltrim(rtrim(c.County))=ltrim(rtrim(p.County))
where p.County not like '%United States%'
''',conn)

#Land it in a CSV for Tableau
NYCovid.to_csv('NYCovid.csv')




