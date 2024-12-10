from sqlalchemy import create_engine

url = "postgresql://postgres:2001@host.docker.internal/IMDB_ETL"

engine = create_engine(url)