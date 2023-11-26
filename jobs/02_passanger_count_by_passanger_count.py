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
        (VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,
         trip_distance,RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,
         payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,
         improvement_surcharge,total_amount) = line.split(',')
        passenger_count = int(passenger_count)
        yield f'{passenger_count:02d}', 1

    def combiner(self, key, values):
        yield key, sum(values)

    def reducer(self, key, values):
        yield key, sum(values)

if __name__=='__main__':
    MRTaxi.run()