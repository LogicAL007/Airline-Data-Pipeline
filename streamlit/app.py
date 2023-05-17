from numpy import double
import streamlit as st
from pandas import DataFrame
import pandas as pd
import pymongo


myclient = pymongo.MongoClient("mongodb://localhost:27017/",username='root',password='example')
mydb = myclient["docstreaming"]
mycol = mydb["flight"] 


# Below the fist chart add a input field for the the source city
source_city = st.sidebar.text_input("Enter your City of Departure:")

# if enter has been used on the input field 
if source_city:

    myquery = {"source_city": source_city}
    # only  excludes
    mydoc = mycol.find(myquery, {"SerialNo": 0, "stops":0,  "arrival_time":0, "duration":0, "days_left":0 } )

    # create dataframe from resulting documents to use drop_duplicates
    df = DataFrame(mydoc)
    
    # drop duplicates, but keep the first one
    df.drop_duplicates(subset ="source_city", keep = 'first', inplace = True)

    # Add the table with a headline
    st.header("Output by Source City")
    table2 = st.dataframe(data=df) 
    

# Below the fist chart add a input field for the destination city
destination_city = st.sidebar.text_input("To check flight details enter serial number:")


# if enter has been used on the input field 
if destination_city:
    
    myquery = {"destination_city": destination_city}
    mydoc = mycol.find( myquery, { "price": 0, "days_left":0, })

    # create the dataframe
    df = DataFrame(mydoc)

    # reindex it so that the columns are order lexicographically 
    reindexed = df.reindex(sorted(df.columns), axis=1)

    # Add the table with a headline
    st.header("Output by Destination City")
    table2 = st.dataframe(data=reindexed) 

# Get data from MongoDB and convert to Pandas DataFrame
dat = pd.DataFrame(list(mycol.find()))

# Create line chart using Plost and Streamlit