# coding: utf-8
import time
import calendar
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import avg
from datetime import datetime
from collections import namedtuple

BIG_TAXI = 's3a://chictaxi/chictaxi.csv'
SMALL_TAXI = 's3a://chictaxi/small.csv'
WEATHER = 's3a://chictaxi/weather.csv'


def get_data(sql_context, path=SMALL_TAXI):
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
            hour = int(date.split(':')[0].split(' ')[-1])
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


def test_string_to_time():
    assert string_to_time('04/13/2017 07:30:00 PM') == datetime.datetime(
        2017, 4, 13, 19, 30)
    assert string_to_time('04/13/2017 07:30:00 AM') == datetime.datetime(
        2017, 4, 13, 7, 30)


# https://spark.apache.org/docs/latest/api/python/pyspark.html?highlight=rdd#pyspark.RDD.sample
# sampled_rdd = taxi_rdd.sample(False, 0.0001, 81)


def q1(rdd):
    """How many taxi records are there?
    How many taxi records for each year of the dataset?
    """
    count = rdd.count()
    yearly_counts = rdd.map(
        lambda x: (getattr(x, 'Trip Start Timestamp').split('/')[-1].split(' ')
                   [0], 1)).reduceByKey(lambda a, b: a + b)

    return count, yearly_counts


def answer_q1(rdd):
    total_records, yearly_counts = q1(rdd)
    counts = yearly_counts.collect()
    return total_records, counts


def q2(rdd, total_records):
    """"How many records in total would you classify as bad?
        Consider a bad record to be one where the Trip Seconds are less than 60,
        but also if the average speed is over 100 mph, the distance is more
        than 1000 miles or the fare is over $2000 (excluding tips, tolls, etc).

        Once you have defined this, ensure that all further answers are based
        only on good data. How many records are “good” by year.


        N.b. for trips under 1 mile, the ave. speed = 0.0 (as miles = 0),
        therefore this is a decent approximation without guarenteeing total
        accuracy as it doesn't take into account the precise
        coordinates of the journey when calculating average speed.



    """
    good_trips = rdd.filter(lambda x: (get(x, 'Trip Seconds') > 60)
                            & (get(x, 'Trip Miles') < 1000)
                            & (get(x, 'Fare') < 2000))
    #                     & ((get(x, 'Trip Miles') / (get(x, 'Trip Seconds') / 60)) < 100)

    return good_trips, total_records - good_trips.count()


def answer_q2(taxi_rdd, total_records):
    good_trips, num_bad = q2(taxi_rdd, total_records)
    num_bad / total_records
    _, good_trips_by_year = q1(good_trips)
    print(f'Bad records: {num_bad} ')
    print(f'Bad percentage: {num_bad / total_records * 100}')
    return good_trips, num_bad, good_trips_by_year


def get_2018_rides(rdd):
    return rdd.filter(lambda x: getattr(x, 'Trip Start Timestamp').split('/')[
        -1].split(' ')[0] == '2018')


def q3(rdd):
    """For each taxi, calculate the average revenue per day excluding tolls (i.e. Fare + Tips).
    Identify the most successful taxi in 2018 in terms of total revenue (Fare + Tips).

    https://stackoverflow.com/questions/29930110/calculating-the-averages-for-each-key-in-a-pairwise-k-v-rdd-in-spark-with-pyth

    """
    return rdd.map(lambda x: (get(
        x, 'Taxi ID'), [get(x, 'Fare') + get(x, 'Tips'), 1])).reduceByKey(
            lambda x, y: (x[0] + y[0], x[1] + y[1])).mapValues(
                lambda v: v[0] / v[1]).takeOrdered(6, key=lambda x: -x[1])


# q3 answer
def answer_q3(rides_2018):
    return q3(rides_2018)


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


    N.b. due to not being able to assign and therefore reuse variables in the
    context of the lambda func, the start time hs to to computed twice in this
    implementation. Whilst the code is very concise and expressive, however it
    is slightly inefficent. A possible refactor is to use a function which
    takes a row rather than the whole RDD and map to this.

    """
    def midpoint(x):
        """Midpoint:  lambda x: (x[0] + (x[1] - x[0]) /2).hour) ,
        where x = (start, end)
        """
        start = string_to_time(get(x, 'Trip Start Timestamp'))
        end = string_to_time(get(x, 'Trip End Timestamp'))
        return start, end, (start + (start - end) / 2).hour

    def avg_speed(x):
        try:
            return ((get(x, 'Trip Miles') /
                     (get(x, 'Trip Seconds'))) * 60) * 60
        except ZeroDivisionError:
            return 0

        Prepared = namedtuple(
            'Prepared',
            ['fare', 'tips', 'avg_speed', 'start', 'end', 'midpoint', 'miles'])
        return rdd.map(lambda x: Prepared(get(x, 'Fare'), get(
            x, 'Tips'), avg_speed(x), *midpoint(x), get(x, 'Trip Miles')))


def q4(df):
    """ Find the max values for the prepared df

    """
    hourly_avgs = df.groupBy('midpoint').agg(avg('avg_speed'), avg('fare'),
                                             avg('tips'))
    rdd = hourly_avgs.rdd

    Results = namedtuple('Results', ['midpoint', 'fare', 'tips', 'avg_speed'])
    results = rdd.map(lambda x: Results(x.midpoint, x[1], x[2], x[3]))

    #   N.b. `or 0` handles comparision of NoneTypes as part of the max function
    max_fare = results.max(key=lambda x: x.fare or 0)
    max_tips = results.max(key=lambda x: x.tips or 0)
    max_avg_speed = results.max(key=lambda x: x.avg_speed or 0)

    return max_fare, max_tips, max_avg_speed


def answer_q4(rides_2018):
    prepared_rdd = prepare_q4(rides_2018)
    prepared_df = prepared_rdd.toDF()
    max_fare, max_tips, max_avg_speed = q4(prepared_df)
    return prepared_rdd, max_fare, max_tips, max_avg_speed


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

    Q5Results = namedtuple('Q5Results', [
        'start', 'month', 'fare', 'tips', 'tip_per_mile',
        'tip_percentage_of_fare'
    ])
    return prepared_rdd.map(lambda x: Q5Results(
        x.start, calendar.month_name[x.start.month], x.fare, x.tips,
        tip_per_mile(x), tips_percentage(x))).sortBy(lambda x: -x.tip_per_mile)


def get_overall_tips_percentage(prepared_rdd):
    try:
        return prepared_rdd.map(lambda x: x.tips.sum() / prepared_rdd.map(
            lambda x: x.fare).sum() * 100)
    except ZeroDivisionError:
        return 0


def get_tips_percentage_per_month(prepared_rdd):
    return prepared_rdd.sortBy(lambda x: x.start)


def answer_q5(prepared_rdd):
    prepared_q5_rdd = prepare_q5(prepared_rdd)
    generous_tippers = prepared_q5_rdd.take(10)
    tippers_by_month = prepared_q5_rdd.sortBy(lambda x: x.start).take(10)
    return prepared_q5_rdd, generous_tippers, tippers_by_month


def prepare_geo_data(rdd):
    Q6Results = namedtuple(
        'Q6Results',
        ['start_lat', 'start_long', 'end_lat', 'end_long', 'start_timestamp'])
    return rdd.map(
        lambda x: Q6Results(get(x, 'Pickup Centroid Latitude'),
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

    vectors = VectorAssembler(inputCols=['start_lat', 'start_long'],
                              outputCol='features',
                              handleInvalid='skip')
    df_ = vectors.transform(df)

    kmeans = KMeans(k=308, seed=1)
    model = kmeans.fit(df_.select('features'))
    predictions = model.transform(df_)
    centers = model.clusterCenters()

    predictions.centers = pd.Series(centers)

    print('Cluster Centers: ')
    for center in centers:
        print(center)

    return predictions, centers


def answer_q6(rides_2018):
    prepared_geo_data = prepare_geo_data(rides_2018)
    predictions, centers = q6(prepared_geo_data.toDF())
    q6_answer = predictions.groupBy('prediction', 'start_lat',
                                    'start_long').count().orderBy(
                                        'count', ascending=False)
    q6_answer.take(5)


def run(sc, sql_context):
    total_records, counts = taxi_rdd = get_data(sql_context)
    answer_q1(taxi_rdd)

    good_trips, num_bad, good_trips_by_year = answer_q2(
        taxi_rdd, total_records)
    rides_2018 = get_2018_rides(good_trips)

    answer_q3(rides_2018)

    prepared_rdd, max_fare, max_tips, max_avg_speed = answer_q4(rides_2018)

    q5_rdd, generous_tippers, tippers_by_month = answer_q5(prepared_rdd)

    answer_q6(rides_2018)


if __name__ == 'main':
    print('hi')
    # conf = SparkConf().setAppName("Q2").setMaster(
    #     "spark://ec2-34-244-39-108.eu-west-1.compute.amazonaws.com:7077")
    sc = SparkContext()
    sql_context = SQLContext(sc)
    start = time.time()
    run(sc, sql_context)
    end = time.time()
    print(f'jobs runtime: {int(end - start)}')
    sc.stop()
