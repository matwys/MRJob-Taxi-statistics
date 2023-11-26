from mrjob.job import MRJob
from mrjob.step import MRStep

class MRTaxi(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer
            )
        ]

    def mapper(self, _, line):
        (VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
         trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID,
         payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
         improvement_surcharge, total_amount) = line.split(',')
        passenger_count = int(passenger_count)
        date_of_pickup, day_time_of_pickup = tpep_pickup_datetime.split(' ')
        hour_of_pickup, minute_of_pickup, second_of_pickup = day_time_of_pickup.split(':')
        yield hour_of_pickup, passenger_count

    def combiner(self, key, values):
        yield key, sum(values)

    def reducer(self, key, values):
        yield key, sum(values)

if __name__=='__main__':
    MRTaxi.run()