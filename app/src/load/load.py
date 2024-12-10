from sqlalchemy import inspect
from sqlalchemy.sql import text
from app.database.connection import engine
import pandas as pd
from datetime import datetime


def load():
    
    try:
        # Tables
        f_movie = 'fact_movie'
        d_genre = 'dim_genre'
        d_details = 'dim_details'
        d_country = 'dim_country'
        d_director = 'dim_director'
        
        
        inspector = inspect(engine)
        
        # FOR DIM GENRE
        if d_genre not in inspector.get_table_names():
            try:
                dim_genre = pd.read_sql_table('temp_dim_genre',con = engine)
                dim_genre.to_sql(name = d_genre, con=engine, if_exists='replace', index=False)
            except Exception as e:
                print("An error occured while creating dim_genre {e}")         
        else:
            try:
                #upsert here
                # dim_genre.to_sql(name='temp_dim_genre', con=engine, if_exists='replace', index=False)
                
                sql = """ 
                    MERGE dim_genre as D
                    USING temp_dim_genre as S
                    ON 
                        D.genre_id = S.genre_id
                    WHEN MATCHED THEN 
                        UPDATE SET 
                            D.genre = S.genre,
                            D.genre_id = S.genre_id
                            
    
                    WHEN NOT MATCHED BY TARGET THEN 
                        INSERT (genre, genre_id) 
                        VALUES (S.genre, S.genre_id);
                """
                # drop = """DROP TABLE temp_dim_genre""" 
                with engine.begin() as conn:
                    conn.execute(text(sql))
                    # conn.execute(text(drop))
                    conn.commit()
            except Exception as e:
                print(f"A error Occured while loading dim_genre {e}")  
        
        # FOR DIM COUNTRY
        if d_country not in inspector.get_table_names():
            try:
                dim_country = pd.read_sql_table('temp_dim_country',con = engine)
                dim_country.to_sql(name=d_country, con=engine, if_exists='replace', index=False)
            except Exception as e:
                print(f"An error Occured while creating dim_country {e}")
        else:
            try:
                #upsert here
                # dim_country.to_sql(name='temp_dim_country', con=engine, if_exists='replace', index=False)
                
                sql = """ 
                    MERGE dim_country as D
                    USING temp_dim_country as S
                    ON 
                        D.country_id = S.country_id
                    WHEN MATCHED THEN 
                        UPDATE SET 
                            D.country = S.country,
                            D.country_id = S.country_id
                            
    
                    WHEN NOT MATCHED BY TARGET THEN 
                        INSERT (country, country_id) 
                        VALUES (S.country, S.country_id);
                """
                # drop = """DROP TABLE temp_dim_country""" 
                with engine.begin() as conn:
                    conn.execute(text(sql))
                    # conn.execute(text(drop))
                    conn.commit()
            
            except Exception as e:
                print(f"An error Occured while loading dim_country {e}")    
        
        # FOR DIM DIRECTOR
        if d_director not in inspector.get_table_names():
            try:
                dim_director = pd.read_sql_table('temp_dim_director',con = engine)
                dim_director.to_sql(name=d_director, con=engine, if_exists='replace', index=False)
                
            except Exception as e:
                print(f"An error Occured while creating dim_director {e}")
        else:
            try:
                #upsert here
                # dim_director.to_sql(name='temp_dim_director', con=engine, if_exists='replace', index=False)
                
                sql = """ 
                    MERGE dim_director as D
                    USING temp_dim_director as S
                    ON 
                        D.director_id = S.director_id
                    WHEN MATCHED THEN 
                        UPDATE SET 
                            D.director = S.director,
                            D.director_id = S.director_id
                            
                    WHEN NOT MATCHED BY TARGET THEN 
                        INSERT (director, director_id) 
                        VALUES (S.director, S.director_id);
                """
                # drop = """DROP TABLE temp_dim_director"""  
                with engine.begin() as conn:
                    conn.execute(text(sql))
                    # conn.execute(text(drop))
                    conn.commit()
                                
            except Exception as e:
                print("An error occured while loading dim_director {e}")
            
        # FOR DIM DETAILS
        if d_details not in inspector.get_table_names():
            try:
                dim_details = pd.read_sql_table('temp_dim_details',con = engine)
                dim_details.to_sql(name=d_details, con=engine, if_exists='replace', index=False) 
            except Exception as e:
               print("An error occured while creating dim_details {e}")
        else:
            try:
                #upsert here
                # dim_details.to_sql(name='temp_dim_details', con=engine, if_exists='replace', index=False)
                
                sql = """ 
                    MERGE dim_details as D
                    USING temp_dim_details as S
                    ON 
                        D.imdb_code = S.imdb_code
                    WHEN MATCHED THEN 
                        UPDATE SET 
                            D.imdb_code = S.imdb_code,
                            D.title = S.title,
                            D.type = S.type,
                            D.certificate_rating = S.certificate_rating
                            
                    WHEN NOT MATCHED BY TARGET THEN 
                        INSERT (imdb_code, title, type, certificate_rating) 
                        VALUES (S.imdb_code, S.title, S.type, S.certificate_rating);
                """
                # drop = """DROP TABLE temp_dim_details"""
                with engine.begin() as conn:
                    conn.execute(text(sql))
                    # conn.execute(text(drop))
                    conn.commit()
            except Exception as e:
                print("An error occured while loading dim_details {e}")
        
        # FOR FACT TABLE
        if f_movie not in inspector.get_table_names():
            try:
                fact_movie = pd.read_sql_table('temp_fact_table',con = engine)
                fact_movie.to_sql(name=f_movie, con=engine, if_exists='replace', index=False)
            except Exception as e:
                print("An error occured while creating fact_table {e}")
        else:
            try:
                #upsert here
                # fact_movie.to_sql(name='temp_fact_movie', con=engine, if_exists='replace', index=False)
                
                sql = """ 
                    MERGE fact_movie as D
                    USING temp_fact_table as S
                    ON 
                        D.imdb_code = S.imdb_code and
                        D.country_id = S.country_id and
                        D.director_id = S.director_id and
                        D.genre_id = S.genre_id 
                    WHEN MATCHED THEN 
                        UPDATE SET 
                            D.imdb_code = S.imdb_code,
                            D.release_year = S.release_year,
                            D.release_month = S.release_month,
                            D.release_day = S.release_day,
                            D.runtime = S.runtime,
                            D.rating = S.rating,
                            D.votecount = S.votecount,
                            D.productionBudget_amount= S.productionBudget_amount,
                            D.worldwideGross_amount= S.worldwideGross_amount,
                            D.genre_id = S.genre_id,
                            D.country_id = S.country_id,
                            D.director_id = S.director_id
                            
                    WHEN NOT MATCHED BY TARGET THEN 
                        INSERT (created_at, imdb_code, release_year, release_month, release_day, runtime, rating, votecount,
                                productionBudget_amount, worldwideGross_amount, genre_id,
                                country_id, director_id,row_id) 
                        VALUES (S.created_at, S.imdb_code, S.release_year, S.release_month,
                                S.release_day, S.runtime, S.rating, S.votecount,
                                S.productionBudget_amount, S.worldwideGross_amount, S.genre_id,
                                S.country_id, S.director_id,S.row_id);
                """
                
                # drop = """DROP TABLE temp_fact_table"""
                with engine.begin() as conn:
                    conn.execute(text(sql))
                    # conn.execute(text(drop))
                    conn.commit()
            except Exception as e:
                print("An error occured while loading fact_table {e}")
                
    except Exception as e:
        print(f'failed: {e}')