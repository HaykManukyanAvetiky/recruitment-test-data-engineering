#!/usr/bin/env python


import csv
import json
import pandas as pd
import logging
import configs as c
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Identity, ForeignKey, Date

logging.getLogger().setLevel(c.LOG_LEVEL)

logging.info('Starting...........')


try:
    logging.info('Connecting to the MySQL...........')
    engine = create_engine(c.CONNECT_DOCK)
    logging.info("Connection successfully..................")
except Exception as err:
    logging.exception("Error while creating engine")      
    

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
logging.info('Structure defined ...')
# dropping structure if exists
TPeople.drop(bind=engine, checkfirst=True)
TCities.drop(bind=engine, checkfirst=True)
TRegions.drop(bind=engine, checkfirst=True)
Tcountries.drop(bind=engine, checkfirst=True)

logging.info('old structure dropped .....')

# Creating structure
meta.create_all(engine)
logging.info('new structure created ....')

# normalizing and reading
places = pd.read_csv(c.PLACES_FILE, encoding=c.ENCODING)

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

people = pd.read_csv(c.PEOPLE_FILE , encoding=c.ENCODING)
people = people.drop_duplicates()
people = people.merge(cities,left_on='place_of_birth', right_on='city_name')\
    .reset_index()[['index','given_name','family_name','date_of_birth','city_id']] # 
people.columns=TPeople.columns.keys()  #final
people.id += 1

logging.info('data normalized ....')

# loading Data

countries.to_sql(Tcountries.fullname, con=engine, if_exists='append', index=False)
regions.to_sql(TRegions.fullname, con=engine, if_exists='append', index=False)
cities.to_sql(TCities.fullname, con=engine, if_exists='append', index=False)
people.to_sql(TPeople.fullname, con=engine, if_exists='append', index=False)

logging.info('data loaded .....')
# exstracting data

with open(c.SQL_FILE) as file:
    sql = file.read()

with open(c.OUTOUT_FILE,'w') as file:
    with engine.connect() as con:
        
        rs = con.execute(sql)     

    json.dump([row._asdict() for row in rs], file, separators=(',', ':'))

logging.info('data extracted and saved .. .. ')
