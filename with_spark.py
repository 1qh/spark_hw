import pyspark
from pyspark.sql import SparkSession
s = SparkSession.builder.getOrCreate()

def spark_shape(df):
    return (df.count(), len(df.columns))
pyspark.sql.dataframe.DataFrame.shape = spark_shape

# Create 2 RDDs
ap_cols = ['ID', 'Name', 'City', 'Country', 'IATA', 'ICAO', 'Latitude', 'Longitude', 'Altitude', 'Timezone', 'DST', 'Tz', 'Type', 'Source']
ap = s.read.csv('airports.dat').toDF(*ap_cols)
ap_rdd = ap.rdd

route_cols = ['Airline', 'AirlineID', 'Source', 'SourceID', 'IATA', 'DestinationID', 'Codeshare', 'Stops', 'Equipment']
route = s.read.csv('routes.dat').toDF(*route_cols)
route_rdd = route.rdd

print(
    'Number of airport:',
    ap[['Name']].distinct().count()
)

print('Number of airport each country:')

ap[['Country']].\
    groupby('Country') \
    .count() \
    .orderBy('count', ascending=False) \
    .show()

# Group by country
ap.groupby('Country').count()

# Filter by country
country_list = ap[['Country']] \
    .distinct() \
    .toPandas()['Country'] \
    .to_list()

for c in country_list:
    print(f'Filter by country {c}')
    ap \
        .filter(ap['Country'] == c) \
        .drop('Country') \
        .show()

print('Number of flights arriving in each country:')

ap[['Country', 'IATA']] \
    .filter(ap['IATA'] != '\\N') \
    .join(
        route \
            .groupBy('IATA') \
            .count(),
        'IATA'
    )[['Country', 'count']] \
    .groupby('Country') \
    .sum() \
    .orderBy('Country') \
    .show()

# Join 2 RDDs
df = ap.join(route, 'IATA')
df_rdd = df.rdd
