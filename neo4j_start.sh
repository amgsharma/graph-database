docker run \
--name neo4j \
-p7474:7474 -p7687:7687 \
-d \
-v $PWD/neo4j/data:/data \
-v $PWD/neo4j/logs:/logs \
-v $PWD/neo4j/import:/var/lib/neo4j/import \
-v $PWD/neo4j/plugins:/plugins \
neo4j:latest
