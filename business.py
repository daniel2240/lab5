from mrjob.job import MRJob
from mrjob.step import MRStep
class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
        company, price,date = line.split(',')
        yield company, float(price)

    def valor_maximo(self, company, price):
        yield max(price), company
    
    def valor_minimo(self, company, price):
        yield min(price), company 
    

    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                reducer = self.valor_maximo),
             ]

if __name__ == '__main__':
    MRWordFrequencyCount.run()