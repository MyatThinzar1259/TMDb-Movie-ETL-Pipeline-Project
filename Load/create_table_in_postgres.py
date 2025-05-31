import os
from dotenv import load_dotenv
from sqlalchemy import (
    create_engine, Column, Integer, String, Float, Date, ForeignKey
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

# === Load environment variables ===
load_dotenv()
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# === SQLAlchemy setup ===
engine = create_engine(
    "postgresql+psycopg2://",
    connect_args={
        "host": DB_HOST,
        "port": DB_PORT,
        "user": DB_USER,
        "password": DB_PASSWORD,
        "dbname": DB_NAME
    }
)
Session = sessionmaker(bind=engine)

Base = declarative_base()

# === Dimension Tables ===
class Movie(Base):
    __tablename__ = 'movie'
    tmdb_id = Column(Integer, primary_key=True)
    title = Column(String)
    facts = relationship("FactMovie", back_populates="movie")

class ProductionCompany(Base):
    __tablename__ = 'production_company'
    company_id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    facts = relationship("FactCompany", back_populates="company")

class Genre(Base):
    __tablename__ = 'genre'
    genre_id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    facts = relationship("FactGenre", back_populates="genre")

class Director(Base):
    __tablename__ = 'director'
    director_id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    facts = relationship("FactDirector", back_populates="director")

class Actor(Base):
    __tablename__ = 'actor'
    actor_id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    facts = relationship("FactActor", back_populates="actor")

class DateDim(Base):
    __tablename__ = 'date_dim'
    release_date = Column(Date, primary_key=True)
    year = Column(Integer)
    month = Column(Integer)
    day = Column(Integer)
    facts = relationship("Fact", back_populates="date_dim")


# === Fact Table ===
class Fact(Base):
    __tablename__ = 'fact'
    fact_id = Column(Integer, primary_key=True)
    tmdb_id = Column(Integer)
    title = Column(String)
    budget = Column(Integer)
    revenue = Column(Integer)
    rating = Column(Float)
    release_date = Column(Date, ForeignKey('date_dim.release_date'))
    original_language = Column(String)
    vote_count = Column(Integer)
    runtime = Column(Float)
    source = Column(String)

    date_dim = relationship("DateDim", back_populates="facts")

    fact_movies = relationship("FactMovie", back_populates="fact")
    fact_companies = relationship("FactCompany", back_populates="fact")
    fact_genres = relationship("FactGenre", back_populates="fact")
    fact_directors = relationship("FactDirector", back_populates="fact")
    fact_actors = relationship("FactActor", back_populates="fact")


# === Bridge Tables ===
class FactMovie(Base):
    __tablename__ = 'fact_movie'
    fact_id = Column(Integer, ForeignKey('fact.fact_id'), primary_key=True)
    movie_id = Column(Integer, ForeignKey('movie.tmdb_id'), primary_key=True)
    fact = relationship("Fact", back_populates="fact_movies")
    movie = relationship("Movie", back_populates="facts")

class FactCompany(Base):
    __tablename__ = 'fact_company'
    fact_id = Column(Integer, ForeignKey('fact.fact_id'), primary_key=True)
    company_id = Column(Integer, ForeignKey('production_company.company_id'), primary_key=True)
    fact = relationship("Fact", back_populates="fact_companies")
    company = relationship("ProductionCompany", back_populates="facts")

class FactGenre(Base):
    __tablename__ = 'fact_genre'
    fact_id = Column(Integer, ForeignKey('fact.fact_id'), primary_key=True)
    genre_id = Column(Integer, ForeignKey('genre.genre_id'), primary_key=True)
    fact = relationship("Fact", back_populates="fact_genres")
    genre = relationship("Genre", back_populates="facts")

class FactDirector(Base):
    __tablename__ = 'fact_director'
    fact_id = Column(Integer, ForeignKey('fact.fact_id'), primary_key=True)
    director_id = Column(Integer, ForeignKey('director.director_id'), primary_key=True)
    fact = relationship("Fact", back_populates="fact_directors")
    director = relationship("Director", back_populates="facts")

class FactActor(Base):
    __tablename__ = 'fact_actor'
    fact_id = Column(Integer, ForeignKey('fact.fact_id'), primary_key=True)
    actor_id = Column(Integer, ForeignKey('actor.actor_id'), primary_key=True)
    fact = relationship("Fact", back_populates="fact_actors")
    actor = relationship("Actor", back_populates="facts")


# === Create Tables ===
def create_tables():
    Base.metadata.create_all(engine)
    print("✅ All tables created successfully.")

if __name__ == "__main__":
    create_tables()
