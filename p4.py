import os

import dotenv
import pyodbc
from py2neo import Graph
from py2neo.bulk import create_nodes, create_relationships
from py2neo.data import Node

dotenv.load_dotenv(override=True)

server = os.environ["TPBDD_SERVER"]
database = os.environ["TPBDD_DB"]
username = os.environ["TPBDD_USERNAME"]
password = os.environ["TPBDD_PASSWORD"]
driver= os.environ["ODBC_DRIVER"]

neo4j_server = os.environ["TPBDD_NEO4J_SERVER"]
neo4j_user = os.environ["TPBDD_NEO4J_USER"]
neo4j_password = os.environ["TPBDD_NEO4J_PASSWORD"]

graph = Graph(neo4j_server, auth=(neo4j_user, neo4j_password))

##### Q1 #####
# Python :
create_nodes(graph.auto(), [Node("Artist", idArtist="nm0000000", primaryName="Cyrille Chevallier", birthYear=2001)], labels={"Artist"})

# Cypher :
# graph.run("""
# CREATE (a:Artist {idArtist: "nm0000000", primaryName: "Cyrille Chevallier", birthYear: 2001});
# """)

# MATCH (a:Artist {primaryName: "Cyrille Chevallier"})
# RETURN a;
# -> (:Artist {idArtist: "nm0000000", birthYear: 2001, primaryName: "Cyrille Chevallier"})


#### Q2 #####
# Python :
create_nodes(graph.auto(), [Node("Film", idFilm="tt00000000", primaryTitle="L'histoire de mon 20 au cours Infrastructure de donnees")], labels={"Film"})

# Cypher :
# graph.run("""
# CREATE (f:Film {idFilm: "tt00000000", primaryTitle: "L'histoire de mon 20 au cours Infrastructure de donnees"});
# """)

# MATCH (a:Film {primaryTitle: "L'histoire de mon 20 au cours Infrastructure de donnees"})
# RETURN a;
# -> (:Film {idFilm: "tt00000000", primaryTitle: "L'histoire de mon 20 au cours Infrastructure de donnees"})


#### Q3 #####
# Python :
create_relationships(
    graph.auto(),
    [("nm0000000", {}, "tt00000000")],
    "ACTED_IN",
    ("Artist", "idArtist"),
    ("Film", "idFilm")
)

# Cypher :
# graph.run("""
# MATCH (a:Artist {idArtist: "nm0000000"}), (f:Film {idFilm: "tt00000000"})
# CREATE (a)-[:ACTED_IN]->(f);
# """)

#### Q4 #####
# Python :
create_nodes(graph.auto(), [Node("Artist", idArtist="nm0000001", primaryName="Laurent Cabaret", birthYear=1970), Node("Artist", idArtist="nm0000002", primaryName="Stéphane Vialle", birthYear=1960)], labels={"Artist"})
create_relationships(
    graph.auto(),
    [("nm0000001", {}, "tt00000000"), ("nm0000002", {}, "tt00000000")],
    "DIRECTED",
    ("Artist", "idArtist"),
    ("Film", "idFilm")
)

# Cypher :
# graph.run("""
# CREATE (a1:Artist {idArtist: "nm0000001", primaryName: "Laurent Cabaret", birthYear: 1970}),
#        (a2:Artist {idArtist: "nm0000002", primaryName: "Stéphane Vialle", birthYear: 1960});
# """)

# graph.run("""
# UNWIND [
#   {artistId: "nm0000001", filmId: "tt00000000"},
#   {artistId: "nm0000002", filmId: "tt00000000"}
# ] AS rel
# MATCH (a:Artist {idArtist: rel.artistId}), (f:Film {idFilm: rel.filmId})
# CREATE (a)-[:DIRECTED]->(f);
# """)

#### Q5 #####
result = graph.run("""
MATCH (n:Artist {primaryName: "Nicole Kidman"})
RETURN n.birthYear;
""")
print(result.data())


#### Q6 #####
result = graph.run("""
MATCH (n:Film)
RETURN n;
""")
print(result.data()[:3])


#### Q7 #####
result = graph.run("""
MATCH (n:Artist)
WHERE n.birthYear = 1963
RETURN n.primaryName;
""")
print(result.data()[:3])

result = graph.run("""
MATCH (n:Artist)
WHERE n.birthYear = 1963
RETURN count(n) AS count;
""")
print(result.data()) #222

#### Q8 #####
result = graph.run("""
MATCH (a:Artist)-[:ACTED_IN]->(f:Film)
WITH a, COUNT(DISTINCT f) AS filmCount
WHERE filmCount > 1
RETURN a.primaryName AS actor, filmCount
""")
print(result.data()[:3])

#### Q9 #####
result = graph.run("""
MATCH (a:Artist)-[r]->(f:Film)
WITH a, COLLECT(DISTINCT type(r)) AS roles
WHERE size(roles) > 1
RETURN a.primaryName AS artiste, roles
""")
print(result.data()[:3])


#### Q10 #####
result = graph.run("""
MATCH (a:Artist)-[r]->(f:Film)
WITH a, f, COLLECT(DISTINCT type(r)) AS roles
WHERE size(roles) > 1
RETURN a.primaryName AS artist, 
       roles,
       f.primaryTitle AS film
""")
print(result.data()[:3])


#### Q11 #####
result = graph.run("""
CALL {
  MATCH (f:Film)<-[:ACTED_IN]-(a:Artist)
  WITH f.primaryTitle AS film, COUNT(DISTINCT a) AS actorCount
  RETURN actorCount
  ORDER BY actorCount DESC
  LIMIT 1
}
WITH actorCount AS maxCount
MATCH (f:Film)<-[:ACTED_IN]-(a:Artist)
WITH f.primaryTitle AS film, COUNT(DISTINCT a) AS actorCount, maxCount
WHERE actorCount = maxCount
RETURN film, actorCount;
""")
print(result.data())

