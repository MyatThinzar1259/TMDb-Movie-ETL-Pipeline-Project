import json
import os
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, ForeignKey, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from dotenv import load_dotenv

# Load DB credentials from .env
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# SQLAlchemy setup
Base = declarative_base()
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

# Define tables
class Movie(Base):
    __tablename__ = 'movies'
    tmdb_id = Column(Integer, primary_key=True)
    title = Column(String)
    budget = Column(Integer)
    revenue = Column(Integer)
    rating = Column(Float)
    vote_count = Column(Integer)
    release_date = Column(Date)
    original_language = Column(String)
    runtime = Column(Float)
    source = Column(String)

class ProductionCompany(Base):
    __tablename__ = 'production_companies'
    company_id = Column(Integer, primary_key=True)
    name = Column(String)

class Genre(Base):
    __tablename__ = 'genres'
    genre_id = Column(Integer, primary_key=True)
    name = Column(String)

class Person(Base):
    __tablename__ = 'persons'
    person_id = Column(Integer, primary_key=True)
    name = Column(String)
    category = Column(String)

class MoviePerson(Base):
    __tablename__ = 'movie_persons'
    tmdb_id = Column(Integer, ForeignKey('movies.tmdb_id'), primary_key=True)
    person_id =   Column(Integer, ForeignKey('persons.person_id'), primary_key=True)

class MovieCompany(Base):
    __tablename__ = 'movie_companies'
    tmdb_id = Column(Integer, ForeignKey('movies.tmdb_id'), primary_key=True)
    company_id = Column(Integer, ForeignKey('production_companies.company_id'), primary_key=True)

class MovieGenre(Base):
    __tablename__ = 'movie_genres'
    tmdb_id = Column(Integer, ForeignKey('movies.tmdb_id'), primary_key=True)
    genre_id = Column(Integer, ForeignKey('genres.genre_id'), primary_key=True)

# Create tables
Base.metadata.create_all(engine)

# Load JSON and insert into database
def load_json_to_table(json_file, model_class):
    with open(json_file, encoding='utf-8') as f:
        data = json.load(f)
        for record in data:
            obj = model_class(**record)
            session.merge(obj)  # insert or update
    session.commit()



# Load all JSON files from normalized_json
DATA_DIR = "Data/normalized_json"

load_json_to_table(f"{DATA_DIR}/movies.json", Movie)
load_json_to_table(f"{DATA_DIR}/production_companies.json", ProductionCompany)
load_json_to_table(f"{DATA_DIR}/genres.json", Genre)
load_json_to_table(f"{DATA_DIR}/persons.json", Person)
load_json_to_table(f"{DATA_DIR}/movie_company.json", MovieCompany)
load_json_to_table(f"{DATA_DIR}/movie_genre.json", MovieGenre)
load_json_to_table(f"{DATA_DIR}/movie_person.json", MoviePerson)

print("All data loaded successfully.")