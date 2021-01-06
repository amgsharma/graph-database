from neo4j import GraphDatabase
import csv
from imdb import IMDb



class MovieExample:

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))
        # create an instance of the IMDb class
        self._ia = IMDb()
            
    def import_movies(self, file):
        with open(file, 'r+') as in_file:
            reader = csv.reader(in_file, delimiter=',')
            next(reader, None)
            with self._driver.session() as session:
                # session.run(
                #     "CREATE CONSTRAINT ON (a:Movie) ASSERT a.movieId IS UNIQUE; ")
                # session.run(
                #     "CREATE CONSTRAINT ON (a:Genre) ASSERT a.genre IS UNIQUE; ")
    
                tx = session.begin_transaction()
    
                i = 0;
                j = 0;
                for row in reader:
                    try:
                        if row:
                            movie_id = row[0].strip()
                            title = row[1].strip()
                            genres = row[2].strip()
                            query = """
                                CREATE (movie:Movie {movieId: $movieId, title: $title})
                                with movie
                                UNWIND $genres as genre
                                MERGE (g:Genre {genre: genre})
                                MERGE (movie)-[:HAS_GENRE]->(g)
                            """
                            tx.run(query, {"movieId": movie_id, "title": title, "genres": genres.split("|")})
                            i += 1
                            j += 1
    
                        if i == 1000:
                            tx.commit()
                            print(j, "lines processed")
                            i = 0
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
                #session.run("CREATE CONSTRAINT ON (a:Person) ASSERT a.name IS UNIQUE;")
                tx = session.begin_transaction()
                i = 0;
                j = 0;
                for row in reader:
                    try:
                        if row:
                            movie_id = row[0].strip()
                            imdb_id = row[1].strip()
                            movie = self._ia.get_movie(imdb_id)
                            self.process_movie_info(movie_info=movie, tx=tx, movie_id=movie_id)
                            i += 1
                            j += 1

                        if i == 10:
                            tx.commit()
                            print(j, "lines processed")
                            i = 0
                            tx = session.begin_transaction()
                    except Exception as e:
                        print(e, row, reader.line_num)
                tx.commit()
                print(j, "lines processed")
    
    def process_movie_info(self, tx, movie_info, movie_id):
        query = """
            MATCH (movie:Movie {movieId: $movieId})
            SET movie.plot = $plot
            FOREACH (director IN $directors | MERGE (d:Person {name: director}) SET d:Director MERGE (d)-[:DIRECTED]->(movie))
            FOREACH (actor IN $actors | MERGE (d:Person {name: actor}) SET d:Actor MERGE (d)-[:ACTS_IN]->(movie))
            FOREACH (producer IN $producers | MERGE (d:Person {name: producer}) SET d:Producer MERGE (d)-[:PRODUCES]->(movie))
            FOREACH (writer IN $writers | MERGE (d:Person {name: writer}) SET d:Writer MERGE (d)-[:WRITES]->(movie))
            FOREACH (genre IN $genres | MERGE (g:Genre {genre: genre}) MERGE (movie)-[:HAS_GENRE]->(g))
        """
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
    
        plot = ''
        if 'plot outline' in movie_info:
            plot = movie_info['plot outline']
    
        tx.run(query, {"movieId": movie_id, "directors": directors, "genres": genres, "actors": actors, "plot": plot,
                    "writers": writers, "producers": producers})


    def import_user_item(self, file):
        with open(file, 'r+') as in_file:
            reader = csv.reader(in_file, delimiter=',')
            next(reader, None)
            with self._driver.session() as session:
                session.run("CREATE CONSTRAINT ON (u:User) ASSERT u.userId IS UNIQUE")

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
                                MERGE (user)-[:RATES {rating: {rating}, timestamp: {timestamp}}]->(movie)
                            """
                            tx.run(query, {"movieId":movie_id, "userId": user_id, "rating":rating, "timestamp": timestamp})
                            i += 1
                        if i == 1000:
                            tx.commit()
                            i = 0
                            tx = session.begin_transaction()
                    except Exception as e:
                        print(e, row, reader.line_num)
                tx.commit()

    def recommendTo(self, userId, k):
        user_VSM = self.get_user_vector(userId)
        movies_VSM = self.get_movie_vectors(userId)
        top_k = self.compute_top_k (user_VSM, movies_VSM, k);
        return top_k
    
    def compute_top_k(self, user, movies, k):
        dtype = [ ('movieId', 'U10'),('value', 'f4')]
        knn_values = np.array([], dtype=dtype)
        for other_movie in movies:
            value = cosine_similarity([user], [movies[other_movie]])
            if value > 0:
                knn_values = np.concatenate((knn_values, np.array([(other_movie, value)], dtype=dtype)))
        knn_values = np.sort(knn_values, kind='mergesort', order='value' )[::-1]
        return np.array_split(knn_values, [k])[0]
    
    def get_user_vector(self, user_id):
        query = """
                    MATCH p=(user:User)-[:WATCHED|:RATES]->(movie)
                    WHERE user.userId = {userId}
                    with count(p) as total
                    MATCH (feature:Feature)
                    WITH feature, total
                    ORDER BY id(feature)
                    MATCH (user:User)
                    WHERE user.userId = {userId}
                    OPTIONAL MATCH (user)-[r:INTERESTED_IN]-(feature)
                    WITH CASE WHEN r IS null THEN 0 ELSE (r.weight*1.0f)/(total*1.0f) END as value
                    RETURN collect(value) as vector
                """
        user_VSM = None
        with self._driver.session() as session:
            tx = session.begin_transaction()
            vector = tx.run(query, {"userId": user_id})
            user_VSM = vector.single()[0]
        print(len(user_VSM))
        return user_VSM;
    
    def get_movie_vectors(self, user_id):
        list_of_moview_query = """
                    MATCH (movie:Movie)-[r:DIRECTED|:HAS_GENRE]-(feature)<-[i:INTERESTED_IN]-(user:User {userId: {userId}})
                    WHERE NOT EXISTS((user)-[]->(movie)) AND EXISTS((user)-[]->(feature))
                    WITH movie, count(i) as featuresCount
                    WHERE featuresCount > 5
                    RETURN movie.movieId as movieId
                """
    
        query = """
                    MATCH (feature:Feature)
                    WITH feature
                    ORDER BY id(feature)
                    MATCH (movie:Movie)
                    WHERE movie.movieId = {movieId}
                    OPTIONAL MATCH (movie)-[r:DIRECTED|:HAS_GENRE]-(feature)
                    WITH CASE WHEN r IS null THEN 0 ELSE 1 END as value
                    RETURN collect(value) as vector;
                """
        movies_VSM = {}
        with self._driver.session() as session:
            tx = session.begin_transaction()
    
            i = 0
            for movie in tx.run(list_of_moview_query, {"userId": user_id}):
                movie_id = movie["movieId"];
                vector = tx.run(query, {"movieId": movie_id})
                movies_VSM[movie_id] = vector.single()[0]
                i += 1
                if i % 100 == 0:
                    print(i, "lines processed")
            print(i, "lines processed")
        print(len(movies_VSM))
        return movies_VSM