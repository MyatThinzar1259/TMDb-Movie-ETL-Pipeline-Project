-- 1. Movies with Genres
SELECT 
    m.tmdb_id,
    m.title,
    g.name AS genre_name
FROM movie m
JOIN movie_genre mg ON m.tmdb_id = mg.tmdb_id
JOIN genre g ON mg.genre_id = g.genre_id;

-- 2. Movies with Production Companies
SELECT 
    m.tmdb_id,
    m.title,
    pc.name AS production_company
FROM movie m
JOIN movie_company mc ON m.tmdb_id = mc.tmdb_id
JOIN production_company pc ON mc.company_id = pc.company_id;

-- 3. Movies with Directors and Actors
SELECT
    m.tmdb_id,
    m.title,
    p.name AS person_name,
    p.category
FROM movie m
JOIN movie_person mp ON m.tmdb_id = mp.tmdb_id
JOIN person p ON mp.person_id = p.person_id
WHERE p.category IN ('Director', 'Actor');

-- 4. Combined: Movies with Genres, Companies, Directors, and Actors
SELECT
    m.tmdb_id,
    m.title,
    g.name AS genre,
    pc.name AS production_company,
    p.name AS person_name,
    p.category
FROM movie m
LEFT JOIN movie_genre mg ON m.tmdb_id = mg.tmdb_id
LEFT JOIN genre g ON mg.genre_id = g.genre_id
LEFT JOIN movie_company mc ON m.tmdb_id = mc.tmdb_id
LEFT JOIN production_company pc ON mc.company_id = pc.company_id
LEFT JOIN movie_person mp ON m.tmdb_id = mp.tmdb_id
LEFT JOIN person p ON mp.person_id = p.person_id
ORDER BY m.tmdb_id;
