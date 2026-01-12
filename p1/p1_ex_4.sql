SELECT TOP 1
    birthYear,
    COUNT(idArtist) AS total
FROM
    dbo.tArtist
WHERE
    birthYear != 0
GROUP BY
    birthYear
ORDER BY
    COUNT(*) DESC

-- On nous demande l'année (et non pas la ou les années), donc on classe par nombre de
-- naissances par année décroissant et on sélectionne la ligne avec le nombre (=compte)
-- le plus élevé, ce qui est simple mais n'affiche pas toutes les années en cas d'égalité
-- 1980 : 477