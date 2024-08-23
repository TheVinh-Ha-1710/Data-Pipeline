from mrjob.job import MRJob
from mrjob.job import MRStep
import json


class TopArtistEachYear(MRJob):

    def mapper(self, _, line):
        (data, sales) = line.strip().split("\t")
        artist, year = json.loads(data)
        sales = float(sales)
        yield (year, artist), sales

    def reducer_sum_sales(self, year_artist, sales):
        year, artist = year_artist
        total_sales = sum(sales)
        yield year, (artist, total_sales)

    def reducer_best_sales(self, year, artist_sales):
        yield year, max(artist_sales)

    def mapper_prepare_for_sorting(self, year, artist_sales):
        sorting_key = f"{9999-int(year):04d}"
        yield sorting_key, (year, artist_sales)

    def reducer_final_output(self, sorting_key, values):
        for year, artist_sales in values:
            yield year, artist_sales

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer_sum_sales),
            MRStep(reducer=self.reducer_best_sales),
            MRStep(
                mapper=self.mapper_prepare_for_sorting,
                reducer=self.reducer_final_output,
            ),
        ]


if __name__ == "__main__":
    TopArtistEachYear.run()
