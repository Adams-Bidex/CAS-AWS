# conncet first into the database Database name is PROFILES
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import json
from pydantic import BaseModel
from typing import List
from bandsProcesor import bandsProcessor
from bandsProcessor2 import bandsProcessorNew
from ClusterWrangler import ClusterBandsWrangler##,ClusterBandFilter
from cachetools import TTLCache
from GeneralDash import DashWrangler


app=FastAPI()

# execute chache functionality
cache = TTLCache(maxsize=100, ttl=3600)  # Cache with a maximum size of 100 and TTL of 1 hour
# execute chache functionality
cachedash = TTLCache(maxsize=100, ttl=3600)  # Cache with a maximum size of 100 and TTL of 1 hour

class Filter(BaseModel):
    divisions: list[str]
    levels: list[str]
    locations: list[str]

# Allow access to the endpoint
origins = ["*"]

app.add_middleware(CORSMiddleware,
                   allow_origins=origins,
                   allow_credentials=True,
                   allow_methods=['*'],
                   allow_headers=['*'])

@app.get("/")
async def BandsData():
    return {'data':'Success'}

@app.get("/bands")
async def BandsData():
    data=bandsProcessor()
    return {'data':data['data']}

@app.post("/band")
async def BandsData(FilteredCriteria: Filter):
    cache_key = str(FilteredCriteria)
    cached_response = cache.get(cache_key)

    if cached_response:
        return cached_response

    data = bandsProcessorNew()
    FilteredCriteria = FilteredCriteria.dict()
    Division = FilteredCriteria["divisions"]
    Level = FilteredCriteria["levels"]
    Location = FilteredCriteria["locations"]
    df = await ClusterBandsWrangler(Division, Level, Location)

    response = {
        'data': data['data'],
        'cluster': df['df'],
        'Summaries': df['Summaries'],
        'SalaryByDivisionSummary':df['SalaryByDivisionSummary'],
        'CountByDivision':df['CountByDivision'],
        'Catch_upByYearSummary':df['Catch_upByYearSummary'],
        'Pop':df['Pop']
    } 

    cache[cache_key] = response

    return response

@app.post("/generalDash")
# async def DashData(FilteredCriteria: Filter):
#     cache_key = str(FilteredCriteria)
#     cached_response = cachedash.get(cache_key)

#     if cached_response:
#         return cached_response

#     FilteredCriteria = FilteredCriteria.dict()
#     Division = FilteredCriteria["divisions"]
#     Level = FilteredCriteria["levels"]
#     Location = FilteredCriteria["locations"]
#     df = await DashWrangler(Division, Level, Location)

#     response = {
#         'cluster': df['df'],
#         'Summaries': df['Summaries'],
#         'SalaryByDivisionSummary':df['SalaryByDivisionSummary'],
#         'CountByLevel':df['CountByLevel'],
#         'SalaryByYearSummary':df['SalaryByYearSummary'],
#         'Pop':df['Pop'],
#         'MedSalaryByYearGender':df['MedSalaryByYearGender'],
#         'MedSalaryByGenderLocation':df['MedSalaryByGenderLocation']
#     } 

#     cachedash[cache_key] = response

#     return response
async def DashData(FilteredCriteria: Filter):
    cache_key = str(FilteredCriteria)

    if cache_key in cachedash:
        return cachedash[cache_key]

    FilteredCriteria = FilteredCriteria.dict()
    Division = FilteredCriteria["divisions"]
    Level = FilteredCriteria["levels"]
    Location = FilteredCriteria["locations"]
    df = await DashWrangler(Division, Level, Location)

    response = {
        'cluster': df['df'],
        'Summaries': df['Summaries'],
        'SalaryByDivisionSummary':df['SalaryByDivisionSummary'],
        'CountByLevel':df['CountByLevel'],
        'SalaryByYearSummary':df['SalaryByYearSummary'],
        'Pop':df['Pop'],
        'MedSalaryByYearGender':df['MedSalaryByYearGender'],
        'MedSalaryByGenderLocation':df['MedSalaryByGenderLocation']
    }

    cachedash[cache_key] = response 

    return response






# from fastapi import FastAPI
# from cachetools import TTLCache
# from pydantic import BaseModel

# app = FastAPI()
# cache = TTLCache(maxsize=100, ttl=3600)  # Cache with a maximum size of 100 and TTL of 1 hour

# class FilteredProfiles(BaseModel):
#     Division: list[str]
#     Level: list[str]
#     Location: list[str]

# # Define the route with caching using cachetools
# @app.post("/employeesProfile")
# async def employeesProfile(FilteredCriteria: FilteredProfiles):
#     cache_key = str(FilteredCriteria)
#     cached_response = cache.get(cache_key)

#     if cached_response:
#         return cached_response

#     # Perform your logic here
#     # Replace this with your own implementation

#     # Get the filter values
#     divisions = FilteredCriteria.Division
#     levels = FilteredCriteria.Level
#     locations = FilteredCriteria.Location

#     # Filter based on the provided values
#     result = filter_employees(divisions, levels, locations)

#     # Create the response
#     response = {"message": "Employees data successfully retrieved", "data": result}

#     # Cache the response
#     cache[cache_key] = response

#     return response


# def filter_employees(divisions: list[str], levels: list[str], locations: list[str]) -> list[dict]:
#     # Implement your filtering logic here
#     # Replace this with your own implementation

#     # Example filtering logic:
#     employees = [
#         {"name": "John", "division": "Operations", "level": "L1", "location": "Berlin"},
#         {"name": "Alice", "division": "Operations", "level": "L2", "location": "Vienna"},
#         {"name": "Bob", "division": "Research", "level": "L3", "location": "Berlin"},
#         {"name": "Eve", "division": "Research", "level": "L1", "location": "Vienna"},
#     ]

#     filtered_employees = []

#     for employee in employees:
#         if employee["division"] in divisions or not divisions:
#             if employee["level"] in levels or not levels:
#                 if employee["location"] in locations or not locations:
#                     filtered_employees.append(employee)

#     return filtered_employees
