import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession, SQLContext, functions, types
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
spark = SparkSession.builder.appName('new stations').getOrCreate()
assert spark.version >= '2.4'  # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext
sqlContext = SQLContext(sc)

# data prepare & clean
taxi_data = spark.read.csv("taxi_traffic.csv", inferSchema=True, header=True)
df = taxi_data.filter(taxi_data['trip_distance'] < 10.0)

df = df.where(df['pickup_latitude'] < '40.817894').where(df['pickup_latitude'] > '40.674611').where(
    df['pickup_longitude'] > '-74.104557').where(df['pickup_longitude'] < '-73.92718')
df = df.na.drop()

# pickup cluster
coord_assembler = VectorAssembler(inputCols=['pickup_latitude', 'pickup_longitude'], outputCol='features')
new_df = coord_assembler.transform(df)

def cluster(num):
    kmeans = KMeans(k=num, seed=1)
    model = kmeans.fit(new_df.select('features'))
    df = model.transform(new_df)
    centers = model.clusterCenters()
    lst = []
    for center in centers:
        lst.append(list(center))
        coord = []
        for i in lst:
            if i != [0.0, 0.0]:
                coord.append(i)
    return coord


# kmeans cluster
kmeans = KMeans(k=10, seed=1)
model = kmeans.fit(new_df.select('features'))
df = model.transform(new_df)
df = df.withColumnRenamed('prediction', 'pickup_cluster')
df = df.drop('features')

pd_df = df.toPandas()
pd_df = pd_df.sample(frac=0.1)
sns.set_style('whitegrid')
sns.lmplot(x='pickup_latitude', y='pickup_longitude', data = pd_df[pd_df['pickup_latitude'] != 0.0],
           fit_reg=False, hue='pickup_cluster', height=10, scatter_kws={'s':100}).fig.suptitle('Pickup Cluster')
#plt.show()


# dropoff cluster
coord_assembler = VectorAssembler(inputCols=['dropoff_latitude', 'dropoff_longitude'], outputCol='features')
new_df1 = coord_assembler.transform(df)
# kmeans cluster
kmeans = KMeans(k=10, seed=1)
model = kmeans.fit(new_df1.select('features'))
df1 = model.transform(new_df1)
df1 = df1.withColumnRenamed('prediction', 'dropoff_cluster')
df1 = df1.drop('features')

pd_df = df1.toPandas()
pd_df = pd_df.sample(frac=0.1)
sns.set_style('whitegrid')
sns.lmplot(x='dropoff_latitude', y='dropoff_longitude', data = pd_df[pd_df['dropoff_latitude'] != 0.0],
           fit_reg=False, hue='dropoff_cluster', height=10, scatter_kws={'s':100}).fig.suptitle('Dropoff Cluster')
#plt.show()



import pandas as pd

ten = cluster(10)
fif = cluster(50)
hun = cluster(100)
data = [['ten',[ten]],['fif',[fif]], ['hun',[hun]]]
#f=pd.DataFrame(data).to_csv("data/q3_data/new_station.csv")
data.toPandas().to_csv("data/q3_data/new_station.csv",header=True)
#print(df)