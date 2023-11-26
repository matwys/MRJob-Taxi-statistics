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

        yield VendorID, float(total_amount)

    def combiner(self, key, values):
        total = 0
        num = 0
        for value in values:
            total += value
            num += 1
        yield key, (total, num)

    def reducer(self, key, values):
        total = 0
        num = 0
        for value in values:
            total += value[0]
            num += value[1]
        yield key, total/num

if __name__=='__main__':
    MRTaxi.run()
