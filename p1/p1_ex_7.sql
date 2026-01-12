WITH ranked_films AS (
    SELECT
        idFilm,
        DENSE_RANK() OVER (
            ORDER BY
                COUNT(DISTINCT idArtist) DESC
        ) AS film_rank
    FROM
        dbo.tJob
    WHERE
        category = 'acted in'
    GROUP BY
        idFilm
)

SELECT
    film.primaryTitle,
    ranked_films.*
FROM
    ranked_films
LEFT JOIN
    dbo.tFilm AS film
ON
    ranked_films.idFilm = film.idFilm
WHERE
    ranked_films.film_rank = 1