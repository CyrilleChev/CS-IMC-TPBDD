WITH film_played_in AS (
    SELECT
        idArtist,
        COUNT(DISTINCT idFilm) AS total_film
    FROM
        dbo.tJob
    WHERE
        category = 'acted in'
    GROUP BY
        idArtist
)


SELECT
    film_played_in.idArtist,
    artists.primaryName,
    film_played_in.total_film
FROM
    film_played_in
LEFT JOIN   -- OU INNER JOIN, dépend de ce que l'on veut
    dbo.tArtist AS artists
ON
    film_played_in.idArtist = artists.idArtist
WHERE
    film_played_in.total_film > 1
    and artists.primaryName IS NULL