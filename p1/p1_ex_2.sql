WITH all_artists AS (
    SELECT
        idArtist
    FROM
        dbo.tJob
    UNION ALL
    SELECT
        idArtist
    FROM
        dbo.tArtist
)

SELECT
    COUNT(DISTINCT idArtist)
FROM
    all_artists