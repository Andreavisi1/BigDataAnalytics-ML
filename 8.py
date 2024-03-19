import os
from time import time
import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import split, col, count, size, format_string, input_file_name, element_at
from pyspark.sql.types import StructType

dataset_path = 'BDAchallenge2324' # 'hdfs://192.168.104.45:9000/user/amircoli/BDA2324'
output_path = 'results' # '/home/amircoli/BDAchallenge2324/results/5'

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

def read_csv(year, station):
    dataframe = spark.read.format('csv') \
        .option('header', 'true') \
        .load('{}/{}/{}'.format(dataset_path, year, station)) \
        .withColumn('year', element_at(split(input_file_name(), '/'), -2).cast('string')) \
        .withColumn('station', element_at(split(input_file_name(), '/'), -1).cast('string')) \
        .withColumn('station', split(col('station'), '.csv')[0])
    return dataframe


def export_csv(dataframe, file_name):
    file_path = '{}/{}'.format(output_path, file_name)
    if not os.path.exists(file_path):
        dataframe.coalesce(1).write.format('csv').option('header', 'true').save(file_path)
    else:
        dataframe.coalesce(1).write.format('csv').mode('overwrite').option('header', 'true').save(file_path)
    print('File .csv esportato con successo in: {}/{}'.format(output_path, file_name))

def export_txt(file_name, result):
    with open('{}/{}'.format(output_path, file_name), "w") as file:
        if isinstance(result, list):
           file.write("\n".join(result))
        else:
           file.write(result)
    print('File .txt esportato con successo in: {}/{}'.format(output_path, file_name))


"""
# Task 1:
# Stampare il numero di misurazioni effettuate per ogni anno per ogni stazione (ordinato per anno e stazione
"""
def first_task(dataframe):
    rows = []
    output_dataframe = dataframe.select('year', 'station') \
        .groupBy('year', 'station') \
        .agg(count('*').alias('measures_count')) \
        .orderBy('year', 'station') 
    for row in output_dataframe.collect():
        row_values = '{}, {}, {}'.format(row['year'], row['station'], row['measures_count'])
        rows.append(row_values)
    export_txt("task1.txt", rows)
    #export_csv(output_dataframe, "task1.csv")

"""
# Task 2:
# Stampare le prime 10 temperature (TMP) con il maggior numero di occorrenze ed il relativo conteggio registrate nell’area evidenziata (ordinate per numero di occorrenze e temperatura)
"""
def second_task(dataframe):
    rows = []
    output_dataframe = dataframe \
        .filter((col('LATITUDE').between(30, 60)) & (col('LONGITUDE').between(-135, -90))) \
        .groupBy('TMP') \
        .agg(count('*').alias('TMP_count')) \
        .orderBy(col('TMP_count').desc(), col('TMP').desc()) \
        .limit(10)
    for row in output_dataframe.collect():
        row_values = '[(60,-135);(30,-90)], {}, {}'.format(float(row['TMP'][1:].replace(',', '.')), row['TMP_count'])
        rows.append(row_values)
    export_txt("task2.txt", rows)
    #export_csv(output_dataframe, "task2.csv")


"""
# Task 3:
# Stampare la stazione con la velocità in nodi che occorre più volte ed il relativo conteggio (ordinando per conteggio, velocità e stazione)
"""
def third_task(dataframe):
    output_dataframe = dataframe \
        .withColumn('WND_speed', split(col('WND'), ',')[1]) \
        .groupBy('station', 'WND_speed') \
        .agg(count('*').alias('WND_speed_count')) \
        .orderBy(col('WND_speed_count').desc(), col('WND_speed').desc(), col('station').asc()) \
        .limit(1) 
    result = '{}, {}, {}'.format(output_dataframe.collect()[0]['station'], \
                                 output_dataframe.collect()[0]['WND_speed'], \
                                 output_dataframe.collect()[0]['WND_speed_count'])
    export_txt("task3.txt", result)
    #export_csv(output_dataframe, "task3.csv")


if __name__ == "__main__":
    starting_time = time()
    schema = StructType([])
    total_dataframe = spark.createDataFrame([], schema)
    for root, dirs, files in sorted(os.walk((dataset_path))):
        for file in sorted(files):
            if (file == '.DS_Store'):
                continue
            dataframe = read_csv(os.path.basename(root), file)
            total_dataframe = total_dataframe.unionByName(dataframe, allowMissingColumns=True)
    first_task(total_dataframe)  
    second_task(total_dataframe)    
    third_task(total_dataframe)
    print('\nTempo di completamento: {} seconds.'.format(time() - starting_time))
