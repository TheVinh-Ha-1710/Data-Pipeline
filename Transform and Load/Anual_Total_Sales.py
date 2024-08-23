from mrjob.job import MRJob
from mrjob.step import MRStep


class TotalSaleEachYearByArtist(MRJob):

    def mapper(self, _, line):
        *artists, year, sales = line.strip().split(",")
        yield (", ".join(artists), year), float(sales)

    def reducer(self, artist_year, sales):
        total_sales = sum(sales)
        yield artist_year, total_sales

    def step(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer)]


if __name__ == "__main__":
    TotalSaleEachYearByArtist.run()
