from mrjob.job import MRJob
from mrjob.step import MRStep
class MRWordFrequencyCount(MRJob):
    #Mapper del numero de peliculas vistas por usuario
    def mapper_numero_peliculas(self, _, line):
        user,movie,rating,genre,date = line.split(',')
        yield user, movie

    #Mapper de rating promedio
    def mapper_rating_promedio(self, _, line):
        user,movie,rating,genre,date = line.split(',')
        yield movie, float(rating)

    #Mapper de usuarios que ven las mismas peliculas
    def mapper_usuarios_pelicula(self, _, line):
        user,movie,rating,genre,date = line.split(',')
        yield date, user

    #Numero de peliculas vistas por usuario
    def numero_peliculas(self, user, movie):
        l = list(movie)
        total = len(l)
        yield user, total
    
    #Cantidad de peliculas por día
    def mayor_dia(self, date, user):
        l_user = list(user)
        total = len(l_user)
        yield  date, total

    #Numero de usuarios que ven una misma película
    def rating_promedio(self, movie, rating):
        l = list(rating)
        promedio = sum(l) / len(l)
        yield movie, promedio


    def steps(self):
        return [
            #Numero de peliculas vistas por usuario
            #MRStep(mapper = self.mapper_numero_peliculas,
            #    reducer = self.numero_peliculas),
            #]
            #Cantidad de películas por día
            #MRStep(mapper = self.mapper_mayor_dia,
            #    reducer = self.mayor_dia),
            #]
            #Rating promedio de las películas
            MRStep(mapper = self.mapper_rating_promedio,
                reducer = self.rating_promedio),
            ]

if __name__ == '__main__':
    MRWordFrequencyCount.run()