# Homework 4: Covid19 Data Analysis on Hadoop
The following commands were used to run the code files assuming the class files were compiled into a jar called 
`Covid19-1.0.jar`
### Commands
#### Hadoop
```bash
hadoop jar Covid19-1.0.jar Covid19_1 /cse532/input/covid19_full_data.csv true /cse532/output_1
hadoop jar Covid19-1.0.jar Covid19_2 /cse532/input/covid19_full_data.csv 2020-01-01 2020-03-31 /cse532/output_2
hadoop jar Covid19-1.0.jar Covid19_3 /cse532/input/covid19_full_data.csv hdfs://quickstart.cloudera:8020/cse532/cache/populations.csv /cse532/output_3
```

#### Spark
```bash
spark-submit --class SparkCovid19_1 Covid19-1.0.jar hdfs://quickstart.cloudera:8020/cse532/input/covid19_full_data.csv 2020-01-01 2020-03-31 hdfs://quickstart.cloudera:8020/cse532/output_spark_1
spark-submit --class SparkCovid19_2 Covid19-1.0.jar hdfs://quickstart.cloudera:8020/cse532/input/covid19_full_data.csv hdfs://quickstart.cloudera:8020/cse532/cache/populations.csv hdfs://quickstart.cloudera:8020/cse532/output_spark_2
```