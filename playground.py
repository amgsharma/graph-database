def import_movies(self, file):
    """
    Page 161
    """
    with open(file, 'r+') as in_file:
        reader = csv.reader(in_file, delimiter=',') #A next(reader, None)
    with self._driver.session() as session: #B
        session.run(
        "CREATE CONSTRAINT ON (a:Movie) ASSERT a.movieId IS UNIQUE; ") #C
        session.run(
        "CREATE CONSTRAINT ON (a:Genre) ASSERT a.genre IS UNIQUE; ") #C
        tx = session.begin_transaction() #D
        i = 0;
        j = 0;
        for row in reader:
            try:
                if row:
                    movie_id = strip(row[0]) title = strip(row[1]) genres = strip(row[2]) query = """ #E
                    CREATE (movie:Movie {movieId: {movieId}, title: {title}}) with movie
                    UNWIND {genres} as genre
                    MERGE (g:Genre {genre: genre})
                    MERGE (movie)-[:HAS_GENRE]->(g)
                    """
                    tx.run(query, {"movieId": movie_id, "title": title, "genres":
                    genres.split("|")})
                    i += 1
                    j += 1
                if i == 1000: #F tx.commit()
                    print(j, "lines processed") i=0
                    tx = session.begin_transaction()
            except Exception as e:
                print(e, row, reader.line_num)
        tx.commit()
        print(j, "lines processed")

def import_movie_details(self, file):
    with open(file, 'r+') as in_file:
        reader = csv.reader(in_file, delimiter=',')
        next(reader, None)
        with self._driver.session() as session:
            session.run("CREATE CONSTRAINT ON (a:Person) ASSERT a.name IS UNIQUE;") #A tx = session.begin_transaction()
            i = 0;
            j = 0;
            for row in reader:
                try:
                    if row:
                        movie_id = strip(row[0]) imdb_id = strip(row[1])
                        movie = self._ia.get_movie(imdb_id) #B self.process_movie_info(movie_info=movie, tx=tx, movie_id=movie_id) #C i += 1
                        j += 1
                    if i == 10: 
                        tx.commit()
                        print(j, "lines processed")
                        i=0
                        tx = session.begin_transaction()
                except Exception as e:
                    print(e, row, reader.line_num)
            tx.commit()
            print(j, "lines processed")


def process_movie_info(self, tx, movie_info, movie_id):
    query = """
        MATCH (movie:Movie {movieId: {movieId}})
        SET movie.plot = {plot}
        FOREACH (director IN {directors} | MERGE (d:Person {name: director}) SET d:Director
        MERGE (d)-[:DIRECTED]->(movie))
        FOREACH (actor IN {actors} | MERGE (d:Person {name: actor}) SET d:Actor MERGE (d)-
        [:ACTS_IN]->(movie))
        FOREACH (producer IN {producers} | MERGE (d:Person {name: producer}) SET d:Producer
        MERGE (d)-[:PRODUCES]->(movie))
        FOREACH (writer IN {writers} | MERGE (d:Person {name: writer}) SET d:Writer MERGE
        (d)-[:WRITES]->(movie))
        FOREACH (genre IN {genres} | MERGE (g:Genre {genre: genre}) MERGE (movie)-
        [:HAS_GENRE]->(g)) """
    directors = []
    for director in movie_info['directors']:
        if 'name' in director.data:
            directors.append(director['name'])
    genres = ''
    if 'genres' in movie_info:
        genres = movie_info['genres']
    actors = []
    for actor in movie_info['cast']:
        if 'name' in actor.data: 
            actors.append(actor['name'])
    writers = []
    for writer in movie_info['writers']:
        if 'name' in writer.data:
            writers.append(writer['name'])
    producers = []
    for producer in movie_info['producers']:
        producers.append(producer['name'])
    plot = '' #E
    if 'plot outline' in movie_info:
        plot = movie_info['plot outline']
    tx.run(query, {"movieId": movie_id, "directors": directors, "genres": genres, "actors": actors, "plot": plot,
    "writers": writers, "producers": producers})


def import_user_item(self, file):
    """
    from page 169
    """
    with open(file, 'r+') as in_file:
        reader = csv.reader(in_file, delimiter=',') next(reader, None)
            with self._driver.session() as session:
                session.run("CREATE CONSTRAINT ON (u:User) ASSERT u.userId IS UNIQUE") #A
                tx = session.begin_transaction()
                i = 0;
                for row in reader:
                    try:
                        if row:
                            user_id = strip(row[0])
                            movie_id = strip(row[1])
                            rating = strip(row[2])
                            timestamp = strip(row[3])
                            query = """
                            MATCH (movie:Movie {movieId: {movieId}})
                            MERGE (user:User {userId: {userId}})
                            MERGE (user)-[:RATES {rating: {rating}, timestamp: {timestamp}}]->movie
                            """
                            tx.run(query, {"movieId":movie_id, 
                                            "userId": user_id,
                                            "rating":rating, 
                                            "timestamp": timestamp}
                                            )
                            i += 1
                        if i == 1000: tx.commit()
                            i=0
                            tx = session.begin_transaction() except Exception as e:
                            print(e, row, reader.line_num) tx.commit()

