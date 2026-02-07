#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[2]:


# Read a sample of the data
prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'


# In[3]:


df = pd.read_csv(prefix + 'yellow_tripdata_2021-01.csv.gz', nrows=100)


# In[4]:


len(df)


# In[5]:


df.head()


# In[6]:


# Check data types
df.dtypes


# In[7]:


# Check data shape
df.shape


# In[9]:


# Specify the data type
dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}


# In[10]:


parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


# In[11]:


df.head()


# In[12]:


df = pd.read_csv(
    prefix + 'yellow_tripdata_2021-01.csv.gz',
    nrows=100,
    dtype=dtype,
    parse_dates=parse_dates
)


# In[13]:


df.head()


# In[14]:


df['tpep_pickup_datetime']


# In[15]:


get_ipython().system('uv add sqlalchemy')


# In[16]:




# In[17]:


from sqlalchemy import create_engine
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')


# In[18]:


print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))


# In[19]:


df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')


# In[20]:


df.head(0)


# In[ ]:




