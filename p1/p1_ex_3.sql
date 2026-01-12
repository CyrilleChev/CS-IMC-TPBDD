SELECT
    primaryName --COUNT(idArtist) pour avoir le compte
FROM
    dbo.tArtist
WHERE
    birthYear = 1960

-- filtrer sur les brthYear en utilisant where, compter les ids pour le compte
-- count : 203