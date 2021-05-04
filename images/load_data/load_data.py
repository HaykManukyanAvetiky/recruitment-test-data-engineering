#!/usr/bin/env python


import csv
import json
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Identity, ForeignKey, Date

print('Starting...........')

connect_alchemy = "mysql+mysqlconnector://codetest:swordfish@database/codetest?charset=utf8" 
# connect_alchemy = "mysql+mysqlconnector://root:12345678@localhost/app2?charset=utf8" 

try:
    print('Connecting to the MySQL...........')
    engine = create_engine(connect_alchemy)
    print("Connection successfully..................")
except Exception as err:
    print("Error while connecting to MySQL:", err)      
    exit()

# defining structure
meta = MetaData()

Tcountries = Table(
   'countries', meta, 
   Column('country_id', Integer,Identity(), primary_key = True), 
   Column('country_name',String(150))
)

TRegions = Table(
   'regions', meta, 
   Column('region_id', Integer, Identity(), primary_key = True), 
   Column('region_name', String(150)),
   Column('country_id', Integer, ForeignKey('countries.country_id'))
)

TCities = Table(
   'cities', meta, 
   Column('city_id', Integer, Identity(), primary_key = True), 
   Column('city_name', String(150)),
   Column('region_id', Integer, ForeignKey('regions.region_id'))
)

TPeople = Table(
   'people', meta, 
   Column('id', Integer, Identity(), primary_key = True), 
   Column('given_name', String(150)),
   Column('family_name', String(150)),
   Column('date_of_birth', Date()),
   Column('birth_city_id', Integer, ForeignKey('cities.city_id'))
)
print('Structure defined ...')
# dropping structure if exists
TPeople.drop(bind=engine, checkfirst=True)
TCities.drop(bind=engine, checkfirst=True)
TRegions.drop(bind=engine, checkfirst=True)
Tcountries.drop(bind=engine, checkfirst=True)

print('old structure dropped .....')

# Creating structure
meta.create_all(engine)
print('new structure created ....')

# normalizing
places = pd.read_csv('/data/places.csv')

regions = places[['region','country']].drop_duplicates().reset_index()
regions.columns = ['region_id','region_name','country_name']
regions.region_id += 1
countries = places['country'].drop_duplicates().reset_index()
countries.columns = Tcountries.columns.keys()  #final
countries.country_id += 1
cities = places.merge(regions,left_on='region', right_on='region_name')[['city','region_id']].reset_index()
cities.columns = TCities.columns.keys()  #final
cities.city_id += 1
regions = regions.merge(countries, on= 'country_name')[TRegions.columns.keys()] #final

people = pd.read_csv('/data/people.csv', encoding="utf-8")
people = people.merge(cities,left_on='place_of_birth', right_on='city_name')\
    .reset_index()[['index','given_name','family_name','date_of_birth','city_id']] # 
people.columns=TPeople.columns.keys()  #final
people.id += 1

print('data normalized ....')

# loading Data

countries.to_sql(Tcountries.fullname, con=engine, if_exists='append', index=False)
regions.to_sql(TRegions.fullname, con=engine, if_exists='append', index=False)
cities.to_sql(TCities.fullname, con=engine, if_exists='append', index=False)
people.to_sql(TPeople.fullname, con=engine, if_exists='append', index=False)

print('data loaded .....')
# exstracting data
with open('/data/output.json','w') as file:
    with engine.connect() as con:
        sql = """select p.given_name, p.family_name, DATE_FORMAT(p.date_of_birth,"%%Y-%%m-%%d") date_of_birth, c.city_name birth_city, 
                    r.region_name birth_region, co.country_name birth_country 
                    FROM people p
                    join cities c on c.city_id = p.Birth_city_id
                    join regions r on r.region_id = c.region_id
                    join countries co on co.country_id = r.country_id
                    order by 1
        """ 
        rs = con.execute(sql)     

    json.dump([row._asdict() for row in rs], file, separators=(',', ':'))
