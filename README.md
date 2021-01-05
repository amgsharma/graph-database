# graph-database

## How To Run

In an ipython shell:

```
from playground import MovieExample
movieExample = MovieExample(uri="bolt://0.0.0.0:7687", user="neo4j", password="test1234")
r = movieExample.import_movies(file="ml-latest-small/movies.csv")
```

This should import the movies but only the movie nodes and their genres. Right now, the code refers to a self._ia, which is described in the book as a way of querying IMDB for more information. 