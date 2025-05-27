CREATE TABLE movie (
    tmdb_id INTEGER PRIMARY KEY,
    title TEXT,
    budget BIGINT,
    revenue BIGINT,
    rating REAL,
    vote_count INTEGER,
    release_date DATE,
    original_language VARCHAR(10),
    runtime INTEGER,
    source TEXT
);

CREATE TABLE production_company (
    company_id SERIAL PRIMARY KEY,
    name TEXT UNIQUE
);

CREATE TABLE genre (
    genre_id SERIAL PRIMARY KEY,
    name TEXT UNIQUE
);

CREATE TABLE person (
    person_id SERIAL PRIMARY KEY,
    name TEXT,
    category VARCHAR(20)  -- Director or Actor
);

CREATE TABLE movie_person (
    tmdb_id INTEGER REFERENCES movie(tmdb_id),
    person_id INTEGER REFERENCES person(person_id),
    PRIMARY KEY (tmdb_id, person_id)
);

CREATE TABLE movie_company (
    tmdb_id INTEGER REFERENCES movie(tmdb_id),
    company_id INTEGER REFERENCES production_company(company_id),
    PRIMARY KEY (tmdb_id, company_id)
);

CREATE TABLE movie_genre (
    tmdb_id INTEGER REFERENCES movie(tmdb_id),
    genre_id INTEGER REFERENCES genre(genre_id),
    PRIMARY KEY (tmdb_id, genre_id)
);

