import pandas as pd
from rich import pretty
import sqlalchemy as sql
import os
pretty.install()
#create directory
os.mkdir(".//Database")
#create database bby creating the type of
# of database needed(e.g MySQL.sqlite,postgres)
# now lets us instatitiate the database

Engine= sql.create_engine("sqlite:///Database/MarketData.sqlite")

# Engine is now the class of sqlite database

# connect to the created database
connection= Engine.connect()
MD=pd.read_csv("workers - ClusterAlln.csv")

# create table by adding profiles  into the database
MD.to_sql("MarketData",con=connection,if_exists="replace")

# read data from database

# open the connection to upload data into created database
# simply create back the databse

# let us initiate database for ClusterMapping

Cluster_Engine= sql.create_engine("sqlite:///Database/Clusters.sqlite")
# connect to the created database
cluster_conn= Cluster_Engine.connect()
Clusters=pd.read_csv("workers - Mapping (1).csv")

# create table by adding profiles  into the database
Clusters.to_sql("Clusters",con=cluster_conn,if_exists="replace")

# let us initiate database for Employee Population

Population_Engine= sql.create_engine("sqlite:///Database/Population.sqlite")
# connect to the created database
pop_conn= Population_Engine.connect()
Population=pd.read_csv("workers - CBS-APP (1).csv")

# create table by adding profiles  into the database
Population.to_sql("Population",con=pop_conn,if_exists="replace")

connection.close()