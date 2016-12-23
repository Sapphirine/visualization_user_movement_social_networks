from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark import SparkFiles, SparkContext
from pyspark.mllib.recommendation import ALS
import math
from time import time

def get_checkin(id):

    print("Get the checkin point of the id " + str(id))
    url = "jdbc:mysql://check-in.cm2toc8pkwqx.us-east-1.rds.amazonaws.com:3306/final"
    spark = SparkSession.builder.appName("GetCheckIn").getOrCreate()

    checkin = spark.read.format("jdbc").option("url",url).option("user","final").option("password","final123").option("dbtable","checkin_new").load()
    checkin.createOrReplaceTempView("checkin_data")

    rows = spark.sql("select round(Latitude, 2) as Lat, round(Longitude, 2) as Long from checkin_data where Person = %s" %(id)).collect()
    points = [(float(row.Lat), float(row.Long)) for row in rows]
    spark.stop()

    sc = SparkContext()
    counts = sc.parallelize(points).map(lambda points: (points, 1)).reduceByKey(lambda a, b: a+b).collect()
    counts = sorted(counts, key=lambda x:x[1], reverse=True)
    totalcount = sum(result[1] for result in counts)
    results = [(result[0][0], result[0][1], round(100.0*result[1]/totalcount, 2)) for result in counts]
    sc.stop()
    return results

def get_pairs(id1, id2):

    print("Get the checkin point of the id " + str(id1) + " " + str(id2))
    url = "jdbc:mysql://check-in.cm2toc8pkwqx.us-east-1.rds.amazonaws.com:3306/final"
    spark = SparkSession.builder.appName("GetPairs").getOrCreate()

    checkin = spark.read.format("jdbc").option("url",url).option("user","final").option("password","final123").option("dbtable","checkin_new").load()
    checkin.createOrReplaceTempView("checkin_data")

    userOne = spark.sql("SELECT Person as a, abstime as aTimeStamp, Time as aTime, Position as aPosition, Longitude as aLongitude, Latitude as aLatitude from checkin_data where Person = %s"%(id1))
    userOne.createOrReplaceTempView("userOne")
    userTwo = spark.sql("SELECT Person as b, abstime as bTimeStamp, Time as bTime, Position as bPosition, Longitude as bLongitude, Latitude as bLatitude from checkin_data where Person = %s"%(id2))
    userTwo.createOrReplaceTempView("userTwo")
    userTogether = spark.sql("SELECT * from userOne join userTwo on round(aLongitude,2) = round(bLongitude,2) and \
            round(aLatitude,2) = round(bLatitude,2) and abs(aTimeStamp - bTimeStamp) < 60")

    pair = userTogether.select('aTime', 'aPosition', 'aLongitude', 'aLatitude', 'bTime', 'bPosition', 'bLongitude', 'bLatitude').collect()
    pairArray = [(i.aTime,i.bTime,round(i.aLatitude,2),round(i.aLongitude,2)) for i in pair]
    points = [(round(i.aLatitude,2),round(i.aLongitude,2)) for i in pair]
    spark.stop()

    sc = SparkContext()
    counts = sc.parallelize(points).map(lambda points: (points, 1)).reduceByKey(lambda a, b: a+b).collect()
    sc.stop()
    print(counts)
    print(pairArray)
    
    return pairArray, counts

def get_social(id, min, max):

    print("Get the social of the id " + str(id))
    url = "jdbc:mysql://check-in.cm2toc8pkwqx.us-east-1.rds.amazonaws.com:3306/final"
    spark = SparkSession.builder.appName("Social").getOrCreate()

    checkin = spark.read.format("jdbc").option("url",url).option("user","final").option("password","final123").option("dbtable","checkin_new").load()
    checkin.createOrReplaceTempView("checkin_data")
    edge = spark.read.format("jdbc").option("url",url).option("user","final").option("password","final123").option("dbtable","edge").load()
    edge.createOrReplaceTempView("edge")

    rows = spark.sql("SELECT DISTINCT round(Latitude, 2) as Lat, round(Longitude, 2) as Lon, abstime from checkin_data where Person = %s" % (id))
    rows.createOrReplaceTempView("dis_pos")
    
    select = ("SELECT a.Person as id from checkin_data a inner join dis_pos b ON round(a.Latitude, 2) = b.Lat \
        and round(a.Longitude, 2) = b.Lon and abs(a.abstime - b.abstime) < 60 and a.Person >= %s and a.Person <= %s and a.Person != %s" % (min, max, id))
    commons = spark.sql(select).collect()
    records = [common.id for common in commons]

    friends = spark.sql("SELECT Person2 from edge where Person1 = %s" % (id)).collect()
    friends = [(friend.Person2) for friend in friends]
    friends = set(friends)
    spark.stop()

    sc = SparkContext()
    counts = sc.parallelize(records).map(lambda points: (points, 1)).reduceByKey(lambda a, b: a+b).collect()
    sc.stop()
    counts = dict(counts)
    counts = {key:counts[key] for key in counts if key in friends}
    results = counts.items()
    results = sorted(results, key=lambda x:x[1], reverse=True)

    return results


def recommend_friends(id):

    print("Get the friend recommendation of the id " + str(id))
    url = "jdbc:mysql://check-in.cm2toc8pkwqx.us-east-1.rds.amazonaws.com:3306/final"
    spark = SparkSession.builder.appName("Friend_Recommend").getOrCreate()

    checkin = spark.read.format("jdbc").option("url",url).option("user","final").option("password","final123").option("dbtable","checkin_new").load()
    checkin.createOrReplaceTempView("checkin_data")
    edge = spark.read.format("jdbc").option("url",url).option("user","final").option("password","final123").option("dbtable","edge").load()
    edge.createOrReplaceTempView("edge")
    

    rows = spark.sql("SELECT DISTINCT Position from checkin_data where Person = %s" % (id))
    rows.createOrReplaceTempView("dis_pos")
    points = spark.sql("SELECT Position from checkin_data where Person = %s" % (id)).collect()
    points = [point.Position for point in points]

    select = ("SELECT a.Person as id, a.Position as pos from checkin_data a inner join dis_pos b ON a.Position = b.Position and a.Person != %s" %(id))
    commons = spark.sql(select).collect()
    records = [(common.id, common.pos) for common in commons]

    friends = spark.sql("SELECT Person2 from edge where Person1 = %s" % (id)).collect()
    friends = [(friend.Person2) for friend in friends]
    friends = set(friends)

    spark.stop()

    sc = SparkContext()
    counts = sc.parallelize(points).map(lambda points: (points, 1)).reduceByKey(lambda a, b: a+b).collect()
    counts = dict(counts)
    
    persons = sc.parallelize(records).map(lambda points: (points, 1)).reduceByKey(lambda a, b: a+b).collect()

    results = sc.parallelize(persons).map(lambda points: (points[0][0], counts[points[0][1]] * points[1])).reduceByKey(lambda a, b: a+b).collect()
    results = dict(results)
    results = {key:results[key] for key in results if key not in friends}
    recommends = results.items()
    recommends = sorted(recommends, key=lambda x:x[1], reverse=True)
    sc.stop()

    return recommends

def recommend_location(id, maxnum):

    print("Get the timely places recommendation of the id " + str(id))
    url = "jdbc:mysql://check-in.cm2toc8pkwqx.us-east-1.rds.amazonaws.com:3306/final"
    spark = SparkSession.builder.appName("Places_Recommend").getOrCreate()

    checkin = spark.read.format("jdbc").option("url",url).option("user","final").option("password","final123").option("dbtable","checkin_new").load()
    checkin.createOrReplaceTempView("checkin_data")

    edge = spark.read.format("jdbc").option("url",url).option("user","final").option("password","final123").option("dbtable","edge").load()
    edge.createOrReplaceTempView("edge_data")

    edge = spark.read.format("jdbc").option("url",url).option("user","final").option("password","final123").option("dbtable","mapping").load()
    edge.createOrReplaceTempView("mapping_data")

    #rows = spark.sql("SELECT DISTINCT Position from checkin_data where Person = %s" % (id))
    #rows.createOrReplaceTempView("dis_pos")
    points = spark.sql("SELECT DISTINCT Position from checkin_data where Person = %s" % (id)).collect()
    points = [point.Position for point in points]
    points = set(points)
    print(points)

    friends = spark.sql("SELECT Person2 from edge_data where Person1 = %s" %(id))
    print(friends.collect())
    friends.createOrReplaceTempView("friends")
    select = ("SELECT a.Position as pos from checkin_data a inner join friends b ON a.Person = b.Person2")
    commons = spark.sql(select)
    commons.createOrReplaceTempView("positions")
    print("finishing")
    counts = spark.sql("SELECT COUNT(pos) as cnt, pos FROM positions GROUP BY pos ORDER BY cnt DESC LIMIT %s" %(maxnum))

    counts.createOrReplaceTempView("counts")
    print("finishing")
    temps = spark.sql("SELECT a.pos as pos, b.Latitude as Latitude, b.Longitude Longitude FROM counts a inner join mapping_data b ON a.pos = b.Position").collect()
    locations = [(temp.pos, (float(temp.Latitude), float(temp.Longitude))) for temp in temps]
    loc = dict(locations)
    select = ("SELECT a.Position as pos, FLOOR(HOUR(a.Time)/6) as timeslot from checkin_data a inner join counts b ON a.Position = b.pos")
    results = spark.sql(select).collect()
    records = [(result.pos, result.timeslot) for result in results]

    spark.stop()

    sc = SparkContext()
    results = sc.parallelize(records).map(lambda points: (points, 1)).reduceByKey(lambda a, b: a+b).collect()
    results = dict(results)
    sc.stop()

    results = {key:results[key] for key in results if key[0] not in points}
    recommends = {(key[0], key[1], loc[key[0]][0], loc[key[0]][1]):results[key] for key in results}
    recommends = recommends.items()
    recommends = sorted(recommends, key=lambda x:x[1], reverse=True)
    print(recommends)

    return recommends

def recommend_places_2(id):
    """
    Reference: https://github.com/jadianes/spark-movie-lens/blob/master/notebooks/building-recommender.ipynb
    """

    print("Get the places recommendation of the id " + str(id))
    url = "jdbc:mysql://check-in.cm2toc8pkwqx.us-east-1.rds.amazonaws.com:3306/final"
    spark = SparkSession.builder.appName("Places_Recommend").getOrCreate()

    checkin = spark.read.format("jdbc").option("url",url).option("user","final").option("password","final123").option("dbtable","checkin_new").load()
    checkin.createOrReplaceTempView("checkin_data")

    edge = spark.read.format("jdbc").option("url",url).option("user","final").option("password","final123").option("dbtable","edge").load()
    edge.createOrReplaceTempView("edge_data")

    points = spark.sql("SELECT Position from checkin_data where Person = %s" % (id)).collect()
    points_all = [point.Position for point in points]
    points = set(points_all)
    points_2 = list(points)

    total = spark.sql("SELECT count(*) as num from checkin_data where Person = %s" % (id)).collect()
    total = [point.num for point in total]
    total = int(total[0])

    select = ("SELECT a.Person as per, a.Position as pos from checkin_data a inner join edge_data b ON a.Person = b.Person2 and b.Person1 = %s" %(id))

    commons = spark.sql(select).collect()
    records = [(common.per,common.pos) for common in commons]

    records_2 = [(i, i) for i in points_2]

    spark.stop()

    sc = SparkContext()
    
    places = sc.parallelize(records).map(lambda points: (points, 1)).reduceByKey(lambda a, b: a+b).collect()
    rates = sc.parallelize(records).map(lambda points: (points[0], 1)).reduceByKey(lambda a, b: a+b).collect() # in order to get rates
    rates = dict(rates)
    rates = sc.parallelize(places).map(lambda points: (points[0][0], points[0][1], math.log10(round(points[1] * 100.0 / rates[points[0][0]],4 + 10)))).collect()
    rates_list = [float(i[2]) for i in rates]
    min_rates = min(rates_list)
    max_rates = max(rates_list)

    rates = sc.parallelize(rates).map(lambda points: (points[0], points[1], max(((points[2] - min_rates) / (max_rates - min_rates) * 5), 1))).cache()
    points_all = sc.parallelize(points_all).map(lambda points: (points,1)).reduceByKey(lambda a,b : a+b).collect()
    points_all = map(lambda x:(id, x[0], x[1] * 10.0/ total), points_all)
    big, smallSet = rates.randomSplit([8, 2], seed=4L)

    training_RDD, validation_RDD, test_RDD = smallSet.randomSplit([6, 2, 2], seed=6L)
    validation_for_predict_RDD = validation_RDD.map(lambda x: (x[0], x[1]))

    test_for_predict_RDD = test_RDD.map(lambda x: (x[0], x[1]))

    seed = 5L
    iterations = 10
    regularization_parameter = 0.1
    ranks = [4, 8, 12]
    errors = [0, 0, 0]
    err = 0
    tolerance = 0.02
   
    min_error = float('inf')
    best_rank = -1
    best_iteration = -1
    for rank in ranks:
        model = ALS.train(training_RDD, rank, seed=seed, iterations=iterations,
                          lambda_=regularization_parameter)
        predictions = model.predictAll(validation_for_predict_RDD).map(lambda r: ((r[0], r[1]), r[2]))
        print (predictions.collect())
        rates_and_preds = validation_RDD.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)
        error = math.sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean())
        errors[err] = error
        err += 1
        print ('For rank %s the RMSE is %s' % (rank, error))
        if error < min_error:
            min_error = error
            best_rank = rank

    print ('The best model was trained with rank %s' % best_rank)
    model = ALS.train(training_RDD, best_rank, seed=seed, iterations=iterations,
                      lambda_=regularization_parameter)
    predictions = model.predictAll(test_for_predict_RDD).map(lambda r: ((r[0], r[1]), r[2]))
    rates_and_preds = test_RDD.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)
    error = math.sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean())
        
    print ('For testing data the RMSE is %s' % (error))
    training_RDD, test_RDD = big.randomSplit([7, 3], seed=0L)

    complete_model = ALS.train(training_RDD, best_rank, seed=seed, iterations=iterations, lambda_=regularization_parameter)

    test_for_predict_RDD = test_RDD.map(lambda x: (x[0], x[1]))

    predictions = complete_model.predictAll(test_for_predict_RDD).map(lambda r: ((r[0], r[1]), r[2]))
    rates_and_preds = test_RDD.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)
    error = math.sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean())

    print ('For testing data the RMSE is %s' % (error))

    complete_places = sc.parallelize(records_2).map(lambda x: ((x[0]),x[1]))

    def get_counts_and_averages(ID_and_ratings_tuple):
        nratings = len(ID_and_ratings_tuple[1])
        return ID_and_ratings_tuple[0], (nratings, float(sum(x for x in ID_and_ratings_tuple[1]))/nratings)
    ID_with_ratings_RDD = (big.map(lambda x: (x[1], x[2])).groupByKey())
    ID_with_avg_ratings_RDD = ID_with_ratings_RDD.map(get_counts_and_averages)
    rating_counts_RDD = ID_with_avg_ratings_RDD.map(lambda x: (x[0], x[1][0]))
    new_user_ratings_RDD = sc.parallelize(points_all)
    complete_data_with_new_ratings_RDD = big.union(new_user_ratings_RDD)
    t0 = time()
    new_ratings_model = ALS.train(complete_data_with_new_ratings_RDD, best_rank, seed=seed, 
                              iterations=iterations, lambda_=regularization_parameter)

    new_user_ratings_ids = map(lambda x: x, points_2) 
    new_user_unrated_RDD = (big.filter(lambda x: x[0] not in new_user_ratings_ids).map(lambda x: (id, x[0])))

    new_user_recommendations_RDD = new_ratings_model.predictAll(new_user_unrated_RDD)

    print (new_user_recommendations_RDD.collect())

    new_user_recommendations_rating_RDD = new_user_recommendations_RDD.map(lambda x: (x.product, x.rating))
    new_user_recommendations_rating_title_and_count_RDD = \
    new_user_recommendations_rating_RDD.join(complete_places).join(rating_counts_RDD)
    new_user_recommendations_rating_title_and_count_RDD.take(3)
    new_user_recommendations_rating_title_and_count_RDD = \
    new_user_recommendations_rating_title_and_count_RDD.map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))

    print (new_user_recommendations_rating_title_and_count_RDD.collect())
    top_select = new_user_recommendations_rating_title_and_count_RDD.filter(lambda r: r[2]>=25).takeOrdered(25, key=lambda x: -x[1])

    print (top_select)
    print ('TOP recommended movies (with more than 25 reviews):\n%s' %
        '\n'.join(map(str, top_select)))
    
    sc.stop()
    
    return rates

if __name__ == '__main__':
    recommend_friends(38)
    recommend_location(12, 10)
    #recommend_places_2(10)

