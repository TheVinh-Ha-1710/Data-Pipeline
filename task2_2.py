from mrjob.job import MRJob
from mrjob.job import MRStep
import json


class TopSellingArtist(MRJob):

    def mapper(self, _, line):
        (data, sales) = line.strip().split("\t")
        artist, _ = json.loads(data)
        sales = float(sales)
        yield artist, sales

    def reducer_sum_sales(self, artist, sales):
        total_sales = sum(sales)
        yield None, (artist, total_sales)

    def reducer_top_5_artists(self, _, artist_sales):
        top_5 = sorted(artist_sales, key=lambda x: x[1], reverse=True)[:5]
        for artist, total_sales in top_5:
            yield artist, total_sales

    def steps(self):
        return (
            MRStep(mapper=self.mapper, reducer=self.reducer_sum_sales),
            MRStep(reducer=self.reducer_top_5_artists),
        )


if __name__ == "__main__":
    TopSellingArtist.run()
