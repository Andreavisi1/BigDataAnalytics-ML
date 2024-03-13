!pip install pyspark

import os
import pyspark

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

base_path = "BDAchallenge2324"

for root, dirs, files in os.walk(base_path):
    s_index = 1
    for file in files:
        if (file == ".DS_Store"):
            continue
        df = spark.read.csv(os.path.join(root, file), header=True)
        print(os.path.basename(root)+ ", " 
             + "s_" + str(s_index) + ", " 
             + str(df.count()))
        s_index += 1 
