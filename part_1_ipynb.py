#!/usr/bin/env python
# coding: utf-8

# In[1]:


import calendar
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import avg, max, sum
from datetime import datetime
from collections import namedtuple
import matplotlib.pyplot as plt
from uszipcode import SearchEngine as ZipCodeEngine
get_ipython().run_line_magic('matplotlib', 'inline')


BIG_TAXI = 's3a://chictaxi/chictaxi.csv'
SMALL_TAXI = 's3a://chictaxi/small.csv'
WEATHER = 's3a://chictaxi/weather.csv'

# sc = SparkContext()
sql_context = SQLContext(sc)


# In[2]:


# Helper functions

def get_data(sql_context, path=BIG_TAXI):
    df = sql_context.read.csv(path, header='true', inferSchema='true')
    return (df, df.rdd)


def get(x, key, default=0):
    return getattr(x, key) or default


def string_to_time(date):
    """E.g. turns '04/13/2017 07:30:00 AM' into datetime.time(6, 15).

    N.b. extra complexity here as time format isn't a simple 24hr clock;
    first convert to PM times to 24 hr format by manipulating the string,
    then convert to DateTime.
    """
    try:
        if 'PM' in date:
            time = date.split(' ')[1]
            hour =int(date.split(':')[0].split(' ')[-1])
            # Don't turn 12.30 into 24.30!
            if hour != 12:
                hour += 12
            hour = str(hour)
            _time = hour + time[2:]
            _date = date.replace(time, _time)[:-3]
        else:
            _date = date[:-3]
        # https://www.journaldev.com/23365/python-string-to-datetime-strptime
        return datetime.strptime(_date, '%m/%d/%Y %H:%M:%S')
    except ValueError:
        date

def avg_speed(x):
    try:
        return ((get(x, 'Trip Miles') / (get(x, 'Trip Seconds'))) * 60) * 60
    except ZeroDivisionError:
        return 0

    Prepared = namedtuple('Prepared', ['fare', 'tips', 'avg_speed', 'start', 'end', 'midpoint', 'miles'])
    return rdd.map(lambda x: Prepared(get(x, 'Fare'), get(x, 'Tips'), avg_speed(x),
                                       *midpoint(x), get(x, 'Trip Miles')))


def test_string_to_time():
    assert string_to_time('04/13/2017 07:30:00 PM') == datetime.datetime(2017, 4, 13, 19, 30)
    assert string_to_time('04/13/2017 07:30:00 AM') == datetime.datetime(2017, 4, 13, 7, 30)


# In[7]:


taxi_df, taxi_rdd = get_data(sql_context)


# In[4]:


# https://spark.apache.org/docs/latest/api/python/pyspark.html?highlight=rdd#pyspark.RDD.sample
sampled_rdd = taxi_rdd.sample(False, 0.0001, 81)


# In[5]:


def q1(rdd):
    """How many taxi records are there?
    How many taxi records for each year of the dataset?
    """
    count = rdd.count()
    yearly_counts = rdd.map(lambda x: (getattr(x, 'Trip Start Timestamp'
                                              ).split('/')[-1].split(' ')[0], 1)).reduceByKey(lambda a,b: a+b)

    return count, yearly_counts



# In[8]:


# q1 answer
# total_records, yearly_counts = q1(sampled_rdd)
total_records, yearly_counts = q1(taxi_rdd)
# yearly_counts.collect()


# In[11]:


def q2(rdd, total_records):
    """"How many records in total would you classify as bad?
        Consider a bad record to be one where the Trip Seconds are less than 60,
        but also if the average speed is over 100 mph, the distance is more
        than 1000 miles or the fare is over $2000 (excluding tips, tolls, etc).

        Once you have defined this, ensure that all further answers are based only on good data.
        How many records are “good” by year


        N.b. for trips under 1 mile, the ave. speed = 0.0 (as miles = 0), therefore this is a decent approximation
        without guarenteeing total accuracy as it doesn't take into account the precise coordinates of the journey
        when calculating average speed.



    """
    good_trips = rdd.filter(lambda x: (get(x, 'Trip Seconds') > 60 )
                    & (get(x, 'Trip Miles') < 1000)
                    & (get(x, 'Fare') < 2000)
                    & (avg_speed(x) < 100))

    return good_trips, total_records - good_trips.count()




# In[12]:


# q2 answer
# good_trips, num_bad = q2(sampled_rdd, total_records)
good_trips, num_bad = q2(taxi_rdd, total_records)


# In[13]:


# num_bad / total_records
_, good_trips_by_year = q1(good_trips)


# In[13]:


print(num_bad)
print(num_bad / total_records *100)


# In[15]:


good_trips_by_year.collect()


# In[14]:


def get_2018_rides(rdd):
    return rdd.filter(lambda x: getattr(x, 'Trip Start Timestamp').split('/')[-1].split(' ')[0] == '2018')


# In[4]:


# rides_2018 = get_2018_rides(good_trips)

# https://spark.apache.org/docs/latest/rdd-programming-guide.html#external-datasets
# Save/ Load 2018 data to avoid having to recreate from scratch
# rides_2018.saveAsPickleFile('2018_rdd')
# rides_2018_df = rides_2018.toDF()
rides_2018 = sc.pickleFile('2018_rdd')


# In[20]:


# tricky because of custom aggregation required by excluding tolls
# def q3(df):
#     df.groupBy('Taxi Id').agg({'Total Price':"avg"}).orderBy('column_name', ascending=False)


# MapReduce approach - not complete
def q3(rdd):
    """For each taxi, calculate the average revenue per day excluding tolls (i.e. Fare + Tips).
    Identify the most successful taxi in 2018 in terms of total revenue (Fare + Tips).

    https://stackoverflow.com/questions/29930110/calculating-the-averages-for-each-key-in-a-pairwise-k-v-rdd-in-spark-with-pyth

    """
    return rdd.map(lambda x: (get(x, 'Taxi ID'), [get(x, 'Fare') + get(x, 'Tips'), 1])
                      ).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1])).mapValues(
        lambda v: v[0]/v[1]).takeOrdered(6, key=lambda x: -x[1])



# In[21]:


# q3 answer
sorted_fares = q3(rides_2018)


# In[26]:


rides_2018.count()


# In[ ]:


def test_q3_aggregation(sorted_fares, id_='50b668c005b90b8a98cb429f7ad632b913158b885e8c0a2948c4ed8a39801ca3027d4b0e3ee313f82046c085dd7ae8b044666fbd612e0ef663700efbf1dcc54a'):
    """Test to verify that aggregation logic is correct for q3 is correct using simple python"""
    fares = rides_2018.filter(lambda x: get(x, 'Taxi ID') == id_).map(lambda x: [get(x, 'Fare') + get(x, 'Tips'), 1]).collect()

    acc = 0
    for x in fares:
        acc += x[0]
    assert acc / len(ble) == sorted_fares.filter(lambda x: x[0] == id_).collect()[0][1]


# In[19]:


# q4

def prepare_q4(rdd):
    """ Taking 1 hour periods throughout the day (from midnight to midnight)
    across the complete dataset, answer the following.
    Where a trip crosses a boundary (where the drop off is in a different period to the pickup),
    assign that trip to the period where the midpoint of the journey happened.

    a. What is the average speed of taxis during each period?
    b. Which is the period where drivers in total earn the most money in
    terms of fares?
    c. Which is the period of the day where drivers in total earn the most
    in tips?


    Approach:

    - Find the create a tuple of start and end-times (S, E)
    - Convert each of these into DateTime instances
    - Find the midpoint, that is the start, + the time delta of the end - start,
    and then set the hour in scope to this hour.


    N.b. due to not being able to assign and therefore reuse variables in the context of the lambda func,
    the start time hs to to computed twice in this implementation. Whilst the code is very concise and expressive,
    however it is slightly inefficent. A possible refactor is to use a function which takes a row rather than the
    whole RDD and map to this.

    """
    def midpoint(x):
        """Midpoint:  lambda x: (x[0] + (x[1] - x[0]) /2).hour) , where x = (start, end) """
        try:
            start = string_to_time(get(x, 'Trip Start Timestamp'))
            end = string_to_time(get(x, 'Trip End Timestamp'))

            return start, end, (start + (start - end) /2).hour
        except:
            raise Exception(f'OH NO! {x}, Start: {start}, end: {end}')




    Prepared = namedtuple('Prepared', ['fare', 'tips', 'avg_speed', 'start', 'end', 'midpoint', 'miles'])


    return rdd.map(lambda x: Prepared(get(x, 'Fare'), get(x, 'Tips'), avg_speed(x),
                                       *midpoint(x), get(x, 'Trip Miles')))


def q4(df):
    """ Find the max values for the prepared df

    """
    #  Various permuations of this implemention were theorised, such as doing as:

    # 1. pure sparksql implemention as below:
    # speedy = hourly_avgs.agg(max('avg_speed'))
    #     tips = hourly_avgs.agg(max('tips'))
    #     fares = hourly_avgs.agg(max('fare'))
    #     return hourly_avgs, tips, fares

    # however, this leads to greater complexity as each each .agg call returns just the datapoint, not the row
    # i.e. cruically the midpoint is lost

    # 2. A simple sort for each feature and then take the first item of each rdd.
    # This was rejected as it is impossible to avoid three sort operations which are not only expense operations
    # in general, but requires wide dependencies reduction, so even more expensive here.


    # 3. A transformation based approach:

    # Transfor the row data into k, v tuples for the relevent features
    # and then find the max of each of these

    #  e.g. transform a given row rdd of format [m, s, f, t] to:
    # [(m,s), (m,f), (m,t)]
    # This would enable calling performing a filter on each position to gain the maxium averages for each parameter.

    hourly_avgs = df.groupBy('midpoint').agg(avg('avg_speed'), avg('fare'), avg('tips'))
#     hourly_avgs.fillna( { 'avg_speed':0, 'fare':0, 'tips':0 } )
    rdd = hourly_avgs.rdd


    Results = namedtuple('Results', ['midpoint', 'fare', 'tips', 'avg_speed'])
    results = rdd.map(lambda x: Results(x.midpoint, x[1], x[2], x[3]))

#   N.b. `or 0` handles comparision of NoneTypes as part of the max function

    max_fare = results.max(key=lambda x: x.fare or 0)
    max_tips = results.max(key=lambda x: x.tips or 0)
    max_avg_speed = results.max(key=lambda x: x.avg_speed or 0 )

    return results, max_fare, max_tips, max_avg_speed


# In[157]:


def test_q4_mid_points():

    start = string_to_time('04/13/2017 07:30:00 AM')
    end = string_to_time('04/13/2017 07:37:00 AM')
    assert (start + (end - start)/2).hour == 7

    start = string_to_time('04/13/2017 09:30:00 AM')
    end = string_to_time('04/13/2017 07:37:00 AM')
    assert (start + (end - start)/2).hour == 14

    start = string_to_time('04/13/2017 11:30:00 PM')
    end = string_to_time('04/18/2017 01:00:00 AM')
    assert (start + (end - start)/2).hour == 0


# In[20]:


# answer q4
prepared_rdd = prepare_q4(rides_2018)
# prepared_rdd.first()

# prepared_rdd.take(5)
prepared_df = prepared_rdd.toDF()
results, max_fare, max_tips, max_avg_speed = q4(prepared_df)


# In[35]:


# print(f'max_fare: {max_fare}')
# print(f'max_tips: {max_tips}')
# print(f'max_avg_speed: {max_avg_speed}')

df = results.toDF()
df.agg({"fare": "max"}).collect()[0]
df.agg({"tips": "max"}).collect()[0]
df.agg({"avg_speed": "max"}).collect()[0]

# df.show()


# In[17]:


prepared_rdd.count()


# In[36]:


def prepare_q5(prepared_rdd):
    """ What is the overall percentage of tips that drivers get?
    Find the top ten trips with the best tip per distance travelled.

    Create a graph of average tip percentage by month for the whole period.

    """
    def tips_percentage(row):
        try:
            return (row.tips / row.fare) * 100
        except ZeroDivisionError:
            return 0


    def tip_per_mile(row):
        try:
            return row.tips / row.miles
        # In the case of a trip of 0 miles, just use the tip amount
        except ZeroDivisionError:
            return row.tips



    Q5Results = namedtuple('Q5Results', ['start', 'month', 'fare', 'tips', 'tip_per_mile', 'tip_percentage_of_fare'])
    return prepared_rdd.map(lambda x: Q5Results(x.start, calendar.month_name[x.start.month], x.fare, x.tips,
                                                                   tip_per_mile(x), tips_percentage(x))
                                               ).sortBy(lambda x: -x.tip_per_mile)



def get_overall_tips_percentage(prepared_rdd):
        try:
            return prepared_rdd.map(lambda x: x.tips.sum() / prepared_rdd.map(lambda x: x.fare).sum() * 100)
        except ZeroDivisionError:
            return 0


def get_tips_percentage_per_month(prepared_rdd):
       return prepared_rdd.sortBy(lambda x: x.start)




# In[ ]:


# Q5 answer
prepared_q5_rdd = prepare_q5(prepared_rdd)
generous_tippers = prepared_q5_rdd.take(10)
tippers_by_month = prepared_q5_rdd.sortBy(lambda x: x.start).take(10)


df =  prepared_q5_rdd.sortBy(lambda x: x.start).toDF().toPandas()
avg = df.groupby('month').mean()


# In[ ]:


# Q5 Plot
figure, axes = plt.subplots(1,1)
# raw_plt = axes.bar(df['start'], df['tip_percentage_of_fare'])
avg_plt = axes.bar(avg.index, avg['tip_percentage_of_fare'])


axes.set_title('Tip Percentge of total fare per month', fontsize=20)
axes.set_xlabel('Month')
axes.set_ylabel('Tip Percentge')
plt.grid()
plt.show()


# In[8]:


# Q6

# N.b. Lat and Long chosen over the Location data as this would reqiure further parsing

def prepare_geo_data(rdd):


    Q6Results = namedtuple('Q6Results', ['start_lat', 'start_long',
                                         'end_lat', 'end_long',
                                         'start_timestamp'])
    return rdd.map(lambda x: Q6Results(get(x, 'Pickup Centroid Latitude'),
                   get(x, 'Pickup Centroid Longitude'),
                   get(x, 'Dropoff Centroid Latitude'),
                   get(x, 'Dropoff Centroid Longitude'),
                   string_to_time(get(x, 'Trip Start Timestamp'))))


# https://spark.apache.org/docs/latest/ml-clustering.html
# https://spark.apache.org/docs/2.2.0/api/python/pyspark.ml.html#pyspark.ml.clustering.KMeans
# https://spark.apache.org/docs/2.2.0/api/python/pyspark.ml.html#pyspark.ml.feature.VectorAssembler
def q6(df):
    import pandas as pd
    from pyspark.ml.clustering import KMeans
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.evaluation import ClusteringEvaluator

    vectors = VectorAssembler(inputCols=['start_lat', 'start_long'],
                              outputCol='features', handleInvalid='skip')
    df_ = vectors.transform(df)

    kmeans = KMeans(k=308, seed=1)
    model = kmeans.fit(df_.select('features'))
    predictions = model.transform(df_)
    centers = model.clusterCenters()

    predictions.centers = pd.Series(centers)

#     evaluator = ClusteringEvaluator()
#     silhouette = evaluator.evaluate(predictions)
#     print(f'Silhouette with squared euclidean distance = {str(silhouette)}')

    print('Cluster Centers: ')
    for center in centers:
        print(center)


    return predictions, centers


# In[11]:


# answer q6
# prepared_geo_data = prepare_geo_data(rides_2018)
# predictions, centers = q6(prepared_geo_data.toDF())
# q6_answer = predictions.groupBy('prediction', 'start_lat', 'start_long').count().orderBy(
#     'count', ascending=False)
q6_answer.take(10)


# In[4]:


# prepared_geo_data.saveAsPickleFile('prepared_geo_data')
prepared_geo_data = sc.pickleFile('prepared_geo_data')


# In[5]:


# plot q6

# https://www.bigendiandata.com/2017-06-27-Mapping_in_Jupyter/

import numpy as np
import matplotlib.image as mpimg

df = q6_answer.toPandas()

df.plot(kind='scatter', x='start_long', y='start_lat', alpha=0.4)
# plt.show()

chicago_img=mpimg.imread('/home/ec2-user/chicago.png')


axes = df.plot(kind="scatter", x="start_long", y="start_lat",
    s=df['count'] *100, label="count",
               cmap=plt.get_cmap("jet"),
               colorbar=True,
               alpha=0.4, figsize=(10,7)
)

plt.imshow(chicago_img, alpha=0.5,
           interpolation='nearest' )
plt.ylabel("Latitude", fontsize=14)
plt.xlabel("Longitude", fontsize=14)

cbar = plt.colorbar()
cbar.set_label('samples in cluster', fontsize=16)

plt.legend(fontsize=8)
plt.show()



# Q7
zip_seacher = ZipCodeEngine(simple_zipcode=True)


def map_postcodes(row):
    """Return a list of tuples of zipcodes for a given trip's start end coords.

    N.b. a coordinate can map to >1 postcode, (by default uszipcode returns 5 addresses per lat/long search),
    however sometimes these results return duplicates. In order to dedupe the results the following method is used:

    0. construct a list of tuples of list of zipcodes pairs
        0.a query the start_lat, start_long and end_lat, end_long coordinates
        0.b assemble (start, end) pairs by zipping the results back

    1. Cast zipcodes to ints, e.g at this point the data resembles:

    [(60640, 60660),
     (60660, 60640),
     (60613, 60626),
     (60657, 60659),
     (60625, 60645)]
     # - n.b. index 0,1 are essentially dupes with the start/end positions switched

    2. sort the zipcode pairs
    3. cast the output to a set to remove dupicate entries:

    {(60613, 60626), (60625, 60645), (60640, 60660), (60657, 60659)}


    """
    # https://pypi.org/project/uszipcode/
    starts = [res.zipcode for res in zip_seacher.by_coordinates(get(row, 'start_lat'), get(row, 'start_long'))]
    ends = [res.zipcode for res in zip_seacher.by_coordinates(get(row, 'start_lat'), get(row, 'start_long'))]

#     starts = [res.zipcode for res in zip_seacher.by_coordinates(start_lat, start_long)]
#     ends = [res.zipcode for res in zip_seacher.by_coordinates(start_lat, start_long)]

    return set([tuple(sorted([int(s), int(e)])) for s,e in zip(starts, ends)])


def q7(rdd):
    Q7Results = namedtuple('Q7Results', ['zipcodes'])
    # TO DO finish this - currently errors, not sure why
    return rdd.map(map_postcodes(x))


# In[72]:


weather_df, weather_rdd = get_data(sql_context, WEATHER)


# In[196]:


# Q8
def prepare_weather_data(rdd):
    Q8Data = namedtuple('Q8Data', ['meaurement_id','rain_interval', 'intensity','station',  'timestamp', ])
    return rdd.map(lambda x: Q8Data( get(x, 'Measurement ID'), get(x, 'Interval Rain'),
                    get(x, 'Rain Intensity'),
                    get(x, 'Station Name').replace(' ', ''),
                    string_to_time(get(x, 'Measurement Timestamp')
                                  )))
                    # gladly TS format matches so this helper can be reused...

def q8(q8_data):
    """Uses a similar approach to question 6, group the data then recast to RDD and
    the max values. """

    grouped = q8_data.toDF().groupby('timestamp').mean()
    rdd = grouped.rdd


    Results = namedtuple('Results', ['timestamp', 'avg_interval', 'avg_intensity'])
    results = rdd.map(lambda x: Results(x.timestamp, x[1], x[2]))

    #   N.b. `or 0` handles comparision of NoneTypes as part of the max function
    max_interval = results.max(key=lambda x: x.avg_interval or 0)
    max_intensity = results.max(key=lambda x: x.avg_intensity or 0)

    return max_interval, max_intensity, rdd


# In[153]:


# answer q8
q8_data = prepare_weather_data(weather_rdd)
max_interval, max_intensity, rain_rdd = q8(q8_data)


# In[275]:


# Q9

def prepare_q9(rain_data_df, taxi_data_df):
    """N.b. this function isn't generic - reqiures DF in correct format to be able to join.
    rain_data_df - e.g. as output from `q8`
    taxi_data_df - e.g. as output from `q4`

    """
    def to_unix_timestamp(isoformat):
        """Correlation not supported on timestamp data, so need to convert timestamps to ints.
        """
        import time
        return int(time.mktime(time.strptime(isoformat, '%Y-%m-%dT%H:%M:%S')))

    # https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=join
    joined = rain_data_df.join(taxi_data_df, taxi_data_df.start == rain_data_df.timestamp)

    Q9Results = namedtuple('Q9Results', ['timestamp', 'avg_interval', 'avg_intensity', 'fare', 'tips'])
    rdd = joined.rdd.map(lambda x: Q9Results(
        to_unix_timestamp(x.timestamp.isoformat()),
        get(x, 'avg(rain_interval)'), get(x, 'avg(intensity)'),
        x.fare, x.tips))
    return rdd

def q9(df):

#     # https://spark.apache.org/docs/2.2.0/ml-statistics.html#correlation
#     from pyspark.ml.stat import Correlation
#     from pyspark.ml.feature import VectorAssembler

#     # convert to vector column first
#     assembler = VectorAssembler(inputCols=df.columns, outputCol=vector_col, handleInvalid='skip')
#     df_vector = assembler.transform(df).select('features')

    # https://people.eecs.berkeley.edu/~jegonzal/pyspark/pyspark.sql.html#pyspark.sql.DataFrame.corr
    fare_corr = df.stat.corr("timestamp", "fare")
    tip_corr = df.stat.corr("timestamp", "tips")
    return fare_corr, tip_corr


# In[277]:


# q9_data = prepare_q9(rain_rdd.toDF(), prepared_rdd.toDF())
fare_corr, tip_corr = q9(q9_data.toDF())


# In[280]:


fare_corr


# In[ ]:




