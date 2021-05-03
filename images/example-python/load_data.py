#!/usr/bin/env python

# import mysql.connector
import csv
import json
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Identity, ForeignKey, Date

# establish connection 
connection_dict = {"host":"localhost",
  "user":"root",
  "password":"12345678",
  "database" : "app2" }

connect_alchemy = "mysql://%s:%s@%s/%s?charset=utf8" % (
    connection_dict['user'],
    connection_dict['password'],
    connection_dict['host'],
    connection_dict['database']
)

try:
    print('Connecting to the MySQL...........')
    engine = create_engine(connect_alchemy)
    print("Connection successfully..................")
except Exception as err:
    print("Error while connecting to MySQL:", err)      
    exit()

# defining structure
meta = MetaData()

TContries = Table(
   'contries', meta, 
   Column('country_id', Integer,Identity(), primary_key = True), 
   Column('country_name',String(150))
)

TRegions = Table(
   'regions', meta, 
   Column('region_id', Integer, Identity(), primary_key = True), 
   Column('region_name', String(150)),
   Column('country_id', Integer, ForeignKey('contries.country_id'))
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

# dropping structure if exists
TPeople.drop(bind=engine, checkfirst=True)
TCities.drop(bind=engine, checkfirst=True)
TRegions.drop(bind=engine, checkfirst=True)
TContries.drop(bind=engine, checkfirst=True)


# Creating structure
meta.create_all(engine)

# normalizing
places = pd.read_csv('data/places.csv')
regions = places[['region','country']].drop_duplicates().reset_index()
regions.columns = ['region_id','region_name','country_name']
contries = places['country'].drop_duplicates().reset_index()
contries.columns = TContries.columns.keys()  #final
cities = places.merge(regions,left_on='region', right_on='region_name')[['city','region_id']].reset_index()
cities.columns = TCities.columns.keys()  #final
regions = regions.merge(contries, on= 'country_name')[TRegions.columns.keys()] #final

people = pd.read_csv('data/people.csv', encoding="utf-8")
people = people.merge(cities,left_on='place_of_birth', right_on='city_name')\
    .reset_index()[['index','given_name','family_name','date_of_birth','city_id']] # 
people.columns=TPeople.columns.keys()  #final

# loading Data
contries.to_sql(TContries.fullname, con=engine, if_exists='append', index=False)
regions.to_sql(TRegions.fullname, con=engine, if_exists='append', index=False)
cities.to_sql(TCities.fullname, con=engine, if_exists='append', index=False)
people.to_sql(TPeople.fullname, con=engine, if_exists='append', index=False)

