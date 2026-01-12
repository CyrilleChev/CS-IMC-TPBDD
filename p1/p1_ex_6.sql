WITH categories_done AS (    
    SELECT
        idArtist,
        COUNT(DISTINCT category) AS total_categories
    FROM
        dbo.tJob
    GROUP BY
        idArtist
)

SELECT
    categories_done.idArtist,
    artists.primaryName,
    categories_done.total_categories
FROM
    categories_done
LEFT JOIN
    dbo.tArtist AS artists
ON
    categories_done.idArtist = artists.idArtist
WHERE
    categories_done.total_categories > 1