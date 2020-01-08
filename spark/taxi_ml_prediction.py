import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, SQLContext, functions, types
from pyspark.sql import SQLContext, functions
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.ml import Pipeline
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('new stations').getOrCreate()
assert spark.version >= '2.4'  # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
sc = spark.sparkContext
sqlContext = SQLContext(sc)


taxi_data = spark.read.csv(r"taxi_traffic.csv", inferSchema=True, header=True)

df = taxi_data.filter(taxi_data['trip_distance'] < 20.0)
df = df.na.drop()

df = df.withColumn("pickup_datetime", from_unixtime(unix_timestamp(df.pickup_datetime, "yyyy-MM-dd HH:mm:ss")))
df = df.withColumn("pickup_hour", hour(df.pickup_datetime))
df = df.withColumn("pickup_month", month(df.pickup_datetime))

time_diff = (functions.unix_timestamp('dropoff_datetime', "yyyy-MM-dd HH:mm:ss") -
            functions.unix_timestamp('pickup_datetime', "yyyy-MM-dd HH:mm:ss"))/60

df = df.withColumn("duration_min", time_diff)
df = df.withColumn("speed_mph", df.trip_distance / (df.duration_min / 60))


features = ['passenger_count', 'trip_distance', 'rate_code', 'payment_type',
            'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'total_amount', 'travel_time',
            'tolls_amount', 'imp_surcharge', 'velocity', 'pickup_hour', 'pickup_month', 'speed_mph']

# change duration min to label
data = df.select(df.duration_min.alias("label"), *features)
data = data.dropna()

# split train and validation data
train, validation = data.randomSplit([0.75, 0.25])
train = train.cache()
validation = validation.cache()

# create a pipeline
assembler = VectorAssembler(inputCols=features, outputCol="unscaled_features")
scaler = StandardScaler(inputCol="unscaled_features", outputCol="features", withStd=True, withMean=False)
lr = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

pipeline = Pipeline(stages=[assembler, scaler, lr])

model = pipeline.fit(train)

# create an evaluator and score the test data
evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="label", metricName="rmse")

prediction = model.transform(validation)

# Root Mean Square Error
rmse = evaluator.evaluate(prediction)
print("RMSE: %.3f" % rmse)
# RMSE: 3.689
