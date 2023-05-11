from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import split
from pyspark.sql.functions import col , lower
import pandas as pd
import matplotlib.pyplot as plt
import upsetplot
import json

#créer une session dans le master
spark = SparkSession.builder.master("spark://172.20.53.96:7077").appName("WDC-stats").getOrCreate()
#spark = SparkSession.builder.master("local").appName("WDC-stats").getOrCreate()
        
#fichiers de config qui permettent de se connecter au serveur de stockage s3 qui contient les fichiers de DataCommons
endpoint_url = 'https://s3.os-bird.glicid.fr/'
aws_access_key_id = 'bbd95ea3c1174caa88345404b84e458f'
aws_secret_access_key = 'eaf2a72ecf9845f583af7f3513c44f25'
hadoopConf = spark._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', aws_access_key_id)
hadoopConf.set('fs.s3a.secret.key', aws_secret_access_key)
hadoopConf.set('fs.s3a.endpoint', endpoint_url)
hadoopConf.set('fs.s3a.path.style.access', 'true')
hadoopConf.set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
hadoopConf.set('spark.worker.cleanup.enabled', 'true')
hadoopConf.set('fs.s3a.committer.name', 'magic')

#types in schema explor
file_path = "/webdatacommons_data/home/farah/schemaTypes.json"

with open(file_path, 'r') as json_file:
    json_data = json_file.read() 

types_in_schema =json.loads(json_data)
#intangibale types list
file_path1 = "/webdatacommons_data/home/farah/toFilterOut.json"
with open(file_path1, 'r') as json_file:
    json_data = json_file.read()

ToFilterOut =json.loads(json_data)

import json
file_path2 = "/webdatacommons_data/home/farah/typosAndNonexistant.json"
with open(file_path2, 'r') as json_file:
    json_data = json_file.read()

toFilterAgain =json.loads(json_data)

lower_types_in_schema=[value.lower() for value in types_in_schema]


########################################################################################### A DF WITH CS COUNTS

readall = spark.read.option("header",True) \
  .csv("s3a://test-out/wdcfix/**")

csall=readall.groupby("pset").agg(f.sum("count").alias('count')).sort(f.desc("count"))

csall.createOrReplaceTempView("CSET")
csall.show(truncate=150)




%%time
from pyspark.sql.functions import col
def findISA(topn = None):
    if(topn is not None):
        param = f"limit {topn}"
    else:
        param = ""
    
    sets = csall.withColumn("pset", split(csall["pset"], " "))
    
    distinct_predicate = sets.selectExpr("explode(pset) as predicate", "count") \
                    .groupBy("predicate").agg(f.sum(f.col("count")).alias("count"))  \
                    .createOrReplaceTempView("newsets")
    distinct_predicate=spark.sql(f"select * from newsets where predicate like '%isa:%' and count > 1 order by count desc {param}")    
    return distinct_predicate


%%time
allCount_10000 = findISA(10000)
allCount_10000.show()



############################################################################################ Just to check types not in schema 
allCount_10000NotSchema = findISA(10000)
notInSchemaAndInvented=allCount_10000NotSchema.filter((lower(col("predicate"))).isin(lower_types_in_schema)==False)
notInSchemaAndInvented.show(30,truncate=150)


notInSchema= notInSchemaAndInvented.filter((col("predicate")).isin(toFilterAgain)==False)
notInSchema.show(30,truncate=150)


################################################################################################################# AVG DF CREATION
# avg values import
avgValues = spark.read.option("header",True) \
  .csv("s3a://test-out/average/top10000/**")

# avg values is a df with average values of predicate filled per type 
avgValues = avgValues.withColumn("average", col("average").cast("float"))
avgValues = avgValues.sort(avgValues.average.desc())
avgValues = avgValues.filter(avgValues.average != -1)
avgValues.show(30, truncate=150)

# keep the types that are not in schema
from pyspark.sql.functions import  lower
avgNoSchema = avgValues.filter(~(lower(avgValues["type"])).isin(types_in_schema))
avgNoSchema.show(30, truncate=150)


# keep the types that are  in schema
avgSchema = avgValues.filter(avgValues["type"].isin(types_in_schema))
avgSchema.show(30, truncate=150)

###################################################################################################################LIMIT 10
#df with all limit 10
valuesLimit10 = spark.read.option("header",True) \
  .csv("s3a://test-out/structuredness/limit10top10000/**")
valuesLimit10 = valuesLimit10.withColumn("average", col("average").cast("float"))
valuesLimit10 = valuesLimit10.sort(valuesLimit10.average.desc())
valuesLimit10.show(30, truncate=150)

#limit 10 filters to keep not in schema only
limit10NoSchema = valuesLimit10.filter(~valuesLimit10["type"].isin(types_in_schema))
limit10NoSchema.show(30, truncate=150) 


#limit 10 filters to keep schema only
limit10Schema = valuesLimit10.filter(valuesLimit10["type"].isin(types_in_schema))
limit10Schema.show(30, truncate=150)

###################################################################################################################LIMIT 20
#df with all limit 20
valuesLimit20 = spark.read.option("header",True) \
  .csv("s3a://test-out/structuredness/limit20top10000/**")

valuesLimit20 = valuesLimit20.withColumn("average", col("average").cast("float"))
valuesLimit20 = valuesLimit20.sort(valuesLimit20.average.desc())
valuesLimit20.show(30, truncate=150)

#limit 20 filters to keep schema only
limit20NoSchema = valuesLimit20.filter(~valuesLimit20["type"].isin(types_in_schema))
limit20NoSchema.show(30, truncate=150) 

limit20Schema = valuesLimit20.filter(valuesLimit20["type"].isin(types_in_schema))
limit20Schema.show(30, truncate=150)


#df with all limit 30
valuesLimit30 = spark.read.option("header",True) \
  .csv("s3a://test-out/structuredness/limit30top10000/**")

valuesLimit30 = valuesLimit30.withColumn("average", col("average").cast("float"))
valuesLimit30 = valuesLimit30.sort(valuesLimit30.average.desc())
valuesLimit30.show(30, truncate=150)

#limit 30 filters to keep schema only
limit30NoSchema = valuesLimit30.filter(~valuesLimit30["type"].isin(types_in_schema))
limit30NoSchema.show(30, truncate=150) 

limit30Schema = valuesLimit30.filter(valuesLimit30["type"].isin(types_in_schema))
limit30Schema.show(30, truncate=150)

#df with no limit
valuesNoLimit = spark.read.option("header",True) \
  .csv("s3a://test-out/structuredness/nolimittop10000/**")

valuesNoLimit = valuesNoLimit.withColumn("average", col("average").cast("float"))
valuesNoLimit = valuesNoLimit.sort(valuesNoLimit.average.desc())
valuesNoLimit.show(30, truncate=150)

#No limit filters to keep schema only
noLimitNoSchema = valuesNoLimit.filter(~valuesNoLimit["type"].isin(types_in_schema))
noLimitNoSchema.show(30, truncate=150) 

noLimitSchema = valuesNoLimit.filter(valuesNoLimit["type"].isin(types_in_schema))
noLimitSchema.show(30, truncate=150)

#######################################################################
#AVG WITH SCHEMA
######################################################################
from pyspark.sql.functions import desc
avgSchema_count=avgSchema.join(allCount_10000,avgSchema.type == allCount_10000.predicate ,"inner").drop("predicate").filter(avgSchema.average != -1).sort(avgSchema.average.desc())
avgSchema_count=avgSchema_count.filter(col('count')>10000)
avgSchema_count=avgSchema_count.sort(desc('count'))
avgSchema_count.printSchema()
avgSchema_count.show(truncate=150)

#######################################################################
#AVG WITH NO SCHEMA 
#####################################################################
avgNoSchema_count=avgNoSchema.join(allCount_10000,avgNoSchema.type == allCount_10000.predicate ,"inner").drop("predicate").filter(avgNoSchema.average != -1).sort(avgNoSchema.average.desc())
avgNoSchema_count=avgNoSchema_count.sort(desc('count'))
avgNoSchema_count.printSchema()
avgNoSchema_count.show(truncate=150)

#######################################################################
#AVG WITH SCHEMA NO INTANGIBALE 
######################################################################
avgSchema_count_noIntangibale = avgSchema_count.filter(~avgSchema_count["type"].isin(ToFilterOut))
avgSchema_count_noIntangibale=avgSchema_count_noIntangibale.sort(desc('count'))
avgSchema_count_noIntangibale.show(30, truncate=150)


########################################################################################################################################LIMIT 10
#######################################################################
#LIMIT 10 WITH  SCHEMA 
#####################################################################
limit10Schema_count=limit10Schema.join(allCount_10000,limit10Schema.type == allCount_10000.predicate ,"inner").drop("predicate").filter(limit10Schema.average != -1).sort(limit10Schema.average.desc())
limit10Schema_count=limit10Schema_count.sort(desc('count'))
limit10Schema_count.printSchema()
limit10Schema_count.show(truncate=150)


#######################################################################
#LIMIT 10 WITH NO SCHEMA 
#####################################################################
limit10NoSchema_count=limit10NoSchema.join(allCount_10000,limit10NoSchema.type == allCount_10000.predicate ,"inner").drop("predicate").filter(limit10NoSchema.average != -1).sort(limit10NoSchema.average.desc())
limit10NoSchema_count=limit10NoSchema_count.sort(desc('count'))
limit10NoSchema_count.printSchema()
limit10NoSchema_count.show(truncate=150)

#######################################################################
#LIMIT 10 WITH NO  INTANGIBALE
######################################################################
limit10Schema_count_noIntangibale = limit10Schema_count.filter(~limit10Schema_count["type"].isin(ToFilterOut))
limit10Schema_count_noIntangibale=limit10Schema_count_noIntangibale.sort(desc('count'))
limit10Schema_count_noIntangibale.show(30, truncate=150)

############################################################################################################################################################ LIMIT 20
#######################################################################
#LIMIT 20 WITH  SCHEMA 
#####################################################################
limit20Schema_count=limit20Schema.join(allCount_10000,limit20Schema.type == allCount_10000.predicate ,"inner").drop("predicate").filter(limit20Schema.average != -1).sort(limit20Schema.average.desc())
limit20Schema_count=limit20Schema_count.sort(desc('count'))
limit20Schema_count.printSchema()
limit20Schema_count.show(truncate=150)

#######################################################################
#LIMIT 20 WITH NO SCHEMA 
#####################################################################
limit20NoSchema_count=limit20NoSchema.join(allCount_10000,limit20NoSchema.type == allCount_10000.predicate ,"inner").drop("predicate").filter(limit20NoSchema.average != -1).sort(limit20NoSchema.average.desc())
limit20NoSchema_count=limit20NoSchema_count.sort(desc('count'))
limit20NoSchema_count.printSchema()
limit20NoSchema_count.show(truncate=150)

#######################################################################
#LIMIT 20 WITH NO  INTANGIBALE
######################################################################
limit20Schema_count_noIntangibale = limit20Schema_count.filter(~limit20Schema_count["type"].isin(ToFilterOut))
limit20Schema_count_noIntangibale=  limit20Schema_count_noIntangibale.sort(desc('count'))
limit20Schema_count_noIntangibale.show(30, truncate=150)

#################################################################################################################################################################### LIMIT 30
#######################################################################
#LIMIT 30 WITH  SCHEMA 
#####################################################################
limit30Schema_count=limit30Schema.join(allCount_10000,limit30Schema.type == allCount_10000.predicate ,"inner").drop("predicate").filter(limit30Schema.average != -1).sort(limit30Schema.average.desc())
limit30Schema_count=limit30Schema_count.sort(desc('count'))
limit30Schema_count.printSchema()
limit30Schema_count.show(truncate=150)

#######################################################################
#LIMIT 30 WITH NO SCHEMA 
#####################################################################
limit30NoSchema_count=limit30NoSchema.join(allCount_10000,limit30NoSchema.type == allCount_10000.predicate ,"inner").drop("predicate").filter(limit30NoSchema.average != -1).sort(limit30NoSchema.average.desc())
limit30NoSchema_count=limit30NoSchema_count.sort(desc('count'))
limit30NoSchema_count.printSchema()
limit30NoSchema_count.show(truncate=150)

#######################################################################
#LIMIT 30 WITH NO  INTANGIBALE
######################################################################
limit30Schema_count_noIntangibale = limit30Schema_count.filter(~limit30Schema_count["type"].isin(ToFilterOut))
limit30Schema_count_noIntangibale=limit30Schema_count_noIntangibale.sort(desc('count'))
limit30Schema_count_noIntangibale.show(30, truncate=150)


#################################################################################################################################################################### NO LIMIT
#######################################################################
#NO LIMIT  WITH  SCHEMA 
#####################################################################
noLimitSchema_count=noLimitSchema.join(allCount_10000,noLimitSchema.type == allCount_10000.predicate ,"inner").drop("predicate").filter(noLimitSchema.average != -1).sort(noLimitSchema.average.desc())
noLimitSchema_count=noLimitSchema_count.sort(desc('count'))
noLimitSchema_count.printSchema()
noLimitSchema_count.show(truncate=150)

#######################################################################
# NO LIMIT 10 WITH NO SCHEMA 
#####################################################################
noLimitNoSchema_count=noLimitNoSchema.join(allCount_10000,noLimitNoSchema.type == allCount_10000.predicate ,"inner").drop("predicate").filter(noLimitNoSchema.average != -1).sort(noLimitNoSchema.average.desc())
noLimitNoSchema_count=noLimitNoSchema_count.sort(desc('count'))
noLimitNoSchema_count.printSchema()
noLimitNoSchema_count.show(truncate=150)

#######################################################################
# NO LIMIT 10 WITH NO  INTANGIBALE
######################################################################
noLimitSchema_count_noIntangibale = noLimitSchema_count.filter(~noLimitSchema_count["type"].isin(ToFilterOut))
noLimitSchema_count_noIntangibale=noLimitSchema_count_noIntangibale.sort(desc('count'))
noLimitSchema_count_noIntangibale.show(30, truncate=150)

#######################################################################
# JOIN 4 TABLES
######################################################################
from pyspark.sql.functions import desc
limit_10 = limit10Schema.withColumnRenamed("average", "Limit10")
limit_10 = limit_10.withColumnRenamed("type", "typeLimit")

limit_20 = limit20Schema.withColumnRenamed("average", "Limit20")
limit_20 = limit_20.withColumnRenamed("type", "typeLimit")

limit_30 = limit30Schema.withColumnRenamed("average", "Limit30")
limit_30 = limit_30.withColumnRenamed("type", "typeLimit")

noLimit = noLimitSchema.withColumnRenamed("average", "without_limit")
noLimit = noLimit.withColumnRenamed("type", "typeLimit")

mergedTables10=avgSchema_count.join(limit_10,avgSchema_count.type == limit_10.typeLimit ,"inner").drop("typeLimit").sort(desc('count'))

mergedTables20=mergedTables10.join(limit_20,mergedTables10.type == limit_20.typeLimit ,"inner").drop("typeLimit").sort(desc('count'))

mergedTables30=mergedTables20.join(limit_30,mergedTables20.type == limit_30.typeLimit ,"inner").drop("typeLimit").sort(desc('count'))

mergedFinalTable=mergedTables30.join(noLimit,mergedTables30.type == noLimit.typeLimit ,"inner").drop("typeLimit").sort(desc('count'))

mergedFinalTable=mergedFinalTable.filter(col('count')>100000)
mergedFinalTable=mergedFinalTable.sort(desc('count'))
mergedFinalTable.printSchema()
mergedFinalTable.show(truncate=150)





import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql.functions import col
orderedDF = mergedFinalTable.orderBy(col('Limit10').desc())
df_pd = orderedDF.limit(15).toPandas()
ax=df_pd.plot(kind='bar' , x='type', y =[ 'Limit10', 'Limit20', 'Limit30', 'without_limit'])
ax.set_title('Top 15 most structured types with the evolution of limits ')
plt.show()


import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql.functions import col


df_pd = mergedFinalTable.limit(15).toPandas()
ax=df_pd.plot(kind='bar' , x='type', y =[ 'Limit10', 'Limit20', 'Limit30', 'without_limit'])
ax.set_title('Top 15 most used values with different structuredness values ')
plt.show()


################################################################################################################################################## DRAW FUNCTION
import pyspark.sql.functions as F
import matplotlib.pyplot as plt
def create_barplot(df, order_by, y_axis, limit=20, title=None, y_label=None):
    # Sort the DataFrame in descending order by the specified column
    top = df.orderBy(F.desc(order_by)).limit(limit)

    # Extract the values for the specified columns
    x_values = top.select("type").rdd.flatMap(lambda x: x).collect()
    y_values = top.select(y_axis).rdd.flatMap(lambda x: x).collect()

    # Set figure size and bar width based on limit
    if limit <= 20:
        figsize = (12, 6)
        width = 0.8
    elif limit <= 30:
        figsize = (16, 8)
        width = 0.7
    else:
        figsize = (20, 10)
        width = 0.6

    # Create the bar plot
    fig, ax = plt.subplots(figsize=figsize)
    ax.bar(x_values, y_values, width=width)
    ax.tick_params(axis="x", rotation=90)

    # Add title and y-axis label
    if title:
        ax.set_title(title)
    if y_label:
        ax.set_ylabel(y_label)

    plt.show()

#1
create_barplot(avgSchema_count, "count", "average", limit=25, title="Top 25 most uesed types in schema with the average preicates", y_label="average predicate values")

#2
create_barplot(avgSchema_count, "count", "count", limit=25, title="Top 25 most uesed types in Schema.org ", y_label="characteristic set count")

#3
create_barplot(avgSchema_count, "average", "average", limit=25, title="Top 25 types with the highest amount of average predicates filled in a characteristic set ", y_label="average predicates in a characteristic set")

#4
create_barplot(limit10Schema_count, "average", "average", limit=25, title="Top 25 types with the highest amount of structuredness limited to 10 ", y_label="structuredness")

#5
create_barplot(limit10Schema_count, "count", "average", limit=25, title="Top 25 types with thier structuredness limited to 10 ", y_label="structuredness")


