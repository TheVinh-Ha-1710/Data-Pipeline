from mrjob.job import MRJob
from mrjob.job import MRStep
import json


class TopSellingEachDecade(MRJob):

    def mapper(self, _, line):
        (data, sales) = line.strip().split("\t")
        artist, year = json.loads(data)
        decade = (int(year) // 10) * 10
        sales = float(sales)
        yield (decade, artist), sales

    def reducer_sum_sales(self, decade_artist, sales):
        total_sales = sum(sales)
        decade, artist = decade_artist
        yield decade, (artist, total_sales)

    def reducer_sort_decades(self, decade, artist_sales):
        yield None, (decade, list(artist_sales))

    def reducer_find_top_3_decade(self, _, decade_artist_sales):
        sorted_decades = sorted(decade_artist_sales, key=lambda x: x[0], reverse=True)
        for decade, artist_sales in sorted_decades:
            top_3 = sorted(artist_sales, key=lambda x: x[1], reverse=True)[:3]
            decade_formatted = f"{decade}-{decade+9}"
            for artist, total_sales in top_3:
                yield decade_formatted, (artist, total_sales)

    def steps(self):
        return (
            MRStep(mapper=self.mapper, reducer=self.reducer_sum_sales),
            MRStep(reducer=self.reducer_sort_decades),
            MRStep(reducer=self.reducer_find_top_3_decade),
        )


if __name__ == "__main__":
    TopSellingEachDecade.run()
