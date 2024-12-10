import pandas as pd
import requests
import uuid
from datetime import datetime,timedelta
from app.database.connection import engine


def extract():
    try:
        api_link = "https://api.apify.com/v2/datasets/6v7wMjCoJEvcg4hZ2/items?token=apify_api_cVz6ZO0XzuFK70IYRkB66jMJAqMjFF1SppIy"
        response = requests.get(api_link)
        response.raise_for_status()
        data = response.json()
        return data
    except Exception as e:
        print(f"An error Occured during getting data from api{e}")

# def flatten_json(y):
#     out = {}

#     def flatten(x,name = ''):
#         if type(x) is dict:
#             for a in x:
#                 flatten(x[a],name+a+'_')
#         elif type(x) is list:
#             i = 0
#             for a in x:
#                 flatten(x[a],name+str(i)+'_')
#                 i+= 1
#         else:
#             out[name[:-1]] = x
#     flatten(y)
#     return out

def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if isinstance(x, dict):
            for a in x: 
                flatten(x[a], name + a + '_')
        elif isinstance(x, list):
            for i, a in enumerate(x):
                flatten(a, name + str(i) + '_')
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

def normalize_nested_json(data):
    flattened_data = [flatten_json(record) for record in data]
    return pd.DataFrame(flattened_data)

def save_to_staging_area():
    try:
        raw_data = extract()
        raw_df = pd.DataFrame(raw_data)
        selected_columns = ['id','titleType','titleText','certificate','releaseDate','runtime','genres','directors',
                            'ratingsSummary','productionBudget','worldwideGross']
        selected_df = raw_df[selected_columns]
        # print(selected_df.head())
        selected_dictionary = selected_df.to_dict(orient='records')
        normalized_df = normalize_nested_json(selected_dictionary)
        normalized_df['created_at'] = datetime.now()
        normalized_df.to_sql('staging_area',con = engine,if_exists='replace',index=False)
        print("Staging Area Created Successfully")
    except Exception as e:
        print(f"Saving to staging area failed {e}")


