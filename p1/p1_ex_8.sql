WITH actor_film_grouped AS (
    SELECT
        idFilm,
        idArtist,
        COUNT(DISTINCT category) AS total_categories
    FROM
        dbo.tJob
    GROUP BY 
        idFilm,
        idArtist
)

SELECT
    actor_film_grouped.idArtist,
    artist.primaryName,
    film.primaryTitle,
    actor_film_grouped.total_categories
FROM
    actor_film_grouped
LEFT JOIN
    dbo.tFilm AS film
ON
    actor_film_grouped.idFilm = film.idFilm
LEFT JOIN
    dbo.tArtist AS artist
ON
    actor_film_grouped.idArtist = artist.idArtist
WHERE
    actor_film_grouped.total_categories > 1