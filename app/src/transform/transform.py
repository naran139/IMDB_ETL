from app.database.connection import engine
import pandas as pd
from app.src.transform.utils import generate_hash,generate_uuid

def rename_column(df):
    columns = [
    'created_at', 'id', 'titleText_text', 'titleType_id', 'certificate_rating', 'releaseDate_year',
    'releaseDate_month', 'releaseDate_day', 'releaseDate_country_text', 'runtime_seconds', 'ratingsSummary_aggregateRating',
    'ratingsSummary_voteCount', 'genres_genres_0_text',
    'genres_genres_1_text', 'genres_genres_2_text', 'genres_genres_3_text', 'genres_genres_4_text',
    'genres_genres_5_text','directors_0_credits_0_name_nameText_text', 'productionBudget_budget_amount',
    'productionBudget_budget_currency', 'worldwideGross_total_amount', 'worldwideGross_total_currency'
    ]

    # Creating a dataframe with selected columns
    rename_selected_df = df[columns].copy()

    rename_selected_df.rename(columns={'id': 'imdb_code','titleText_text': 'title','titleType_id': 'type','releaseDate_year': 'release_year',
                       'releaseDate_month': 'release_month','releaseDate_day': 'release_day','runtime_seconds': 'runtime','ratingsSummary_aggregateRating': 'rating',
                       'ratingsSummary_voteCount': 'votecount','genres_genres_0_text': 'genre0',
                       'genres_genres_1_text': 'genre1','genres_genres_2_text': 'genre2', 'releaseDate_country_text':'country',
                       'genres_genres_3_text': 'genre3','genres_genres_4_text': 'genre4','genres_genres_5_text': 'genre5',
                       'directors_0_credits_0_name_nameText_text':'director','productionBudget_budget_amount': 'productionBudget_amount',
                       'productionBudget_budget_currency': 'productionBudget_currency','worldwideGross_total_amount': 'worldwideGross_amount',
                       'worldwideGross_total_currency': 'worldwideGross_currency'}, inplace=True)
    


    return rename_selected_df

def melt_genres(rename_selected_df):
    melted_df = rename_selected_df.melt(id_vars=['created_at' , 'imdb_code', 'title', 'type', 'certificate_rating', 'release_year',
                                 'release_month', 'release_day','country','runtime', 'rating', 'votecount', 'director',
                                 'productionBudget_amount', 'productionBudget_currency', 'worldwideGross_amount',
                                 'worldwideGross_currency'],
                        value_vars=['genre0', 'genre1', 'genre2', 'genre3', 'genre4', 'genre5'],
                        var_name='genre_column', value_name='genre')
    
    melted_df = melted_df.dropna(subset = ['genre'])

    # Dropping the genre_column 
    melted_df = melted_df.drop(columns = ['genre_column'])

    return melted_df


def handling_null_value(melted_df): 
    median_impute = ['rating']
    ffil_impute = ['release_year','release_month','release_day']
    zero_impute = ['productionBudget_amount','worldwideGross_amount']
    mode_impute = ['productionBudget_currency','worldwideGross_currency','genre']
    unknown_impute = ['title','type','country','director']

    for column in melted_df.columns:
        if column in median_impute:
            melted_df[column] = melted_df[column].fillna(melted_df[column].median())
        
        elif column in ffil_impute:
            melted_df[column] = melted_df[column].ffill()

        elif column in zero_impute:
            melted_df[column] = melted_df[column].fillna(0)

        elif column in mode_impute:
            melted_df[column] = melted_df[column].fillna(melted_df[column].mode().iloc[0])

        elif column in unknown_impute:
            melted_df[column] = melted_df[column].fillna('UnKnown')
    
    melted_df['certificate_rating'] = melted_df['certificate_rating'].fillna('Not Rated')

    return melted_df 
        
def create_dimension_table(df,columns,column_id=None,generate_hash=None):
    dim_table = df[columns].drop_duplicates()
    dim_table.reset_index(inplace = True,drop= True)
    if column_id and generate_hash:
        dim_table[column_id] = dim_table.apply(generate_hash,axis = 1)
    
    return dim_table

def create_fact_table(df,dim_tables,merge_keys,drop_columns):
    for dim_table,merge_key in zip(dim_tables,merge_keys):
        df = df.merge(dim_table, left_on = merge_key, right_on = merge_key, how='inner')
    
    df.drop(columns = drop_columns ,inplace = True)

    df['row_id'] = df.apply(lambda _: generate_uuid(),axis = 1)

    return df


def transform():
    query ="""
            SELECT * 
            FROM staging_area
            WHERE created_at = (
            SELECT MAX(created_at) 
            FROM staging_area 
            )"""
    
    df = pd.read_sql(query,con= engine)

    rename_selected_df = rename_column(df)

    melted_df = melt_genres(rename_selected_df)

    null_handled_df = handling_null_value(melted_df)

    dim_genre = create_dimension_table(null_handled_df,['genre'],'genre_id',generate_hash)
    dim_director = create_dimension_table(null_handled_df,['director'],'director_id',generate_hash)
    dim_country = create_dimension_table(null_handled_df,['country'],'country_id',generate_hash)
    dim_details = create_dimension_table(null_handled_df,['imdb_code','title','certificate_rating','type'])

    dim_tables = [dim_genre,dim_director,dim_country,dim_details]
    merge_keys = ['genre','director','country','imdb_code']

    drop_columns = ['title_x', 'type_x', 'certificate_rating_x', 'country', 'director', 
                    'productionBudget_currency', 'worldwideGross_currency', 'genre', 
                    'title_y', 'type_y', 'certificate_rating_y']
    fact_movie = create_fact_table(null_handled_df, dim_tables, merge_keys, drop_columns)
    fact_movie.head(10)

    try:
        dim_country.to_sql('temp_dim_country',con = engine,if_exists = "replace", index = False)
        dim_details.to_sql('temp_dim_details',con = engine ,if_exists = "replace", index = False)
        dim_director.to_sql('temp_dim_director',con = engine, if_exists = "replace", index = False)
        dim_genre.to_sql('temp_dim_genre', con = engine, if_exists = "replace", index = False)
        fact_movie.to_sql('temp_fact_table',con = engine, if_exists = "replace", index = False)
    except Exception as e:
        print(f"An error occured{e}")



