from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingCount(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper, 
                reducer=self.reducer
            ),
            MRStep(
                reducer=self.sorter
            )
        ]

    def mapper(self, _, line):
        (userId, movieId, rating, ratingTimestamp) = line.split('\t')
        yield int(movieId), int(rating)

    def reducer(self, movieId, ratings):
        yield None, (len(list(ratings)), movieId)

    def sorter(self, _, ratingCountPairs):
        for ratingCount, movieId in sorted(ratingCountPairs, reverse=True):
            yield movieId, ratingCount

if __name__ == '__main__':
    RatingCount.run()