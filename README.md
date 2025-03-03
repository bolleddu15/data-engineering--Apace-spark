# data-engineering--Apace-spark
I always work on getting meaningful data and utilizing high performance tools like spark which can work parallel on big data 
# **BigData Engineering  Project 1**

![](https://img.shields.io/badge/%F0%9F%94%96-Employee%20Attrition%20Analysis%20--%20Human%20Resouce%20Domain-blueviolet)

## 🤖 Tech-Stack

![](https://github.com/Subham2S/BigData-Engineering-Capstone-Project-1/blob/main/Tech-Stack.jpg)

## 📜 Summary

One of the big corporations needed data engineering services for a decade's worth of employee data. All employee datasets from that period were provided in six CSV files. The first step of this project was to create an Entity Relation Diagram and create a database in an RDBMS with all the tables for structuring and holding the data as per the relations between the tables. So, I imported the CSVs into a MySQL database, transferred to HDFS/Hive in an optimized format, and analyzed with Hive, Impala, Spark, and SparkML. Finally, I created a Bash Script to facilitate the end-to-end data pipeline and machine learning pipeline for automation purposes.

## 🔢 Process

Importing data from MySQL RDBMS to HDFS using Sqoop, Creating HIVE Tables with compressed file format (avro), Explanatory Data Analysis with Impala & SparkSQL and Building Random Forest Classifer Model & Logistic Regression Model using SparkML.


```console
$ sh /home/anabig114212/Capstone_Inputs/Capstone_P1.sh
```

### Linux Commands

```bash
find . -name "*.avsc" -exec rm {}  \;
```

- Removes the metadata of the tables which are there in the Root dir (created by the sqoop command when the code was run last time)

```bash
find . -name "*.java" -exec rm {}  \;
```

- Removes the Java MapReduce Codes which are there in the Root dir (created by the sqoop command when the code was run last time)

```bash
rm -r /home/anabig114212/Capstone_Outputs
mkdir -p /home/anabig114212/Capstone_Outputs
```

- Removes the current `Capstone_Outputs` Folder and Creates a new dir `Capstone_Outputs` - Here all the Outputs will be stored.

```bash
cp -r /home/anabig114212/Capstone_Inputs/* /home/anabig114212/
```

- Recursively Copies everything to root folder to avoid permission issues at later point of time.

### MySQL (`.sql`)

```bash
mysql -u anabig114212 -pBigdata123 -D anabig114212 -e 'source CreateMySQLTables.sql' > /home/anabig114212/Capstone_Outputs/Cap_MySQLTables.txt
```

- Creates MySQL tables & Inserts data. For more details, please check out [`CreateMySQLTables.sql`](https://github.com/Subham2S/BigData-Engineering-Capstone-Project-1/blob/main/Capstone_Inputs/CreateMySQLTables.sql)

```bash
hdfs dfs -rm -r /user/anabig114212/hive/warehouse/Capstone
hdfs dfs -mkdir -p /user/anabig114212/hive/warehouse/Capstone
```

- Removes & Creates the `Warehouse/Capstone` dir to avoid anomalies between same named files

### sqoop

```bash
sqoop import-all-tables --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig114212 --username anabig114212 --password Bigdata123 --compression-codec=snappy --as-avrodatafile --warehouse-dir=/user/anabig114212/hive/warehouse/Capstone --m 1 --driver com.mysql.jdbc.Driver
```

- Importing Data & Metadata of all the Tables from MySQL RDBMS system to Hadoop using SQOOP command

### HDFS Commands

```bash
hdfs dfs -rm -r /user/anabig114212/hive/avsc
hdfs dfs -mkdir -p /user/anabig114212/hive/avsc
hdfs dfs -put  departments.avsc /user/anabig114212/hive/avsc/departments.avsc
hdfs dfs -put  titles.avsc /user/anabig114212/hive/avsc/titles.avsc
hdfs dfs -put  employees.avsc /user/anabig114212/hive/avsc/employees.avsc
hdfs dfs -put  dept_manager.avsc /user/anabig114212/hive/avsc/dept_manager.avsc
hdfs dfs -put  dept_emp.avsc /user/anabig114212/hive/avsc/dept_emp.avsc
hdfs dfs -put  salaries.avsc /user/anabig114212/hive/avsc/salaries.avsc
hadoop fs -chmod +rwx /user/anabig114212/hive/avsc/*
hadoop fs -chmod +rwx /user/anabig114212/hive/warehouse/Capstone/*
```

- Transfering the Metadata to HDFS for Table creation in Hive

### Hive (`.hql`)

```bash
hive -f HiveDB.hql > /home/anabig114212/Capstone_Outputs/Cap_HiveDB.txt
```

- All the hive Tables are created as AVRO format. In the [`HiveDB.hql`](https://github.com/Subham2S/BigData-Engineering-Capstone-Project-1/blob/main/Capstone_Inputs/HiveDB.hql) file Table location and its metadata (schema) locations are mentioned separately.

### Impala (`.sql`)

```bash
impala-shell -i ip-10-1-2-103.ap-south-1.compute.internal -f EDA.sql > /home/anabig114212/Capstone_Outputs/Cap_ImpalaAnalysis.txt
```

- Explanatory Data Analysis is done with Impala. For more details, please check out [`EDA.sql`](https://github.com/Subham2S/BigData-Engineering-Capstone-Project-1/blob/main/Capstone_Inputs/EDA.sql)

```bash
hive -f HiveTables.sql > /home/anabig114212/Capstone_Outputs/Cap_HiveTables.txt
```

- Checking all the records of the Hive Tables before moving to spark. For more details, please check out [`HiveTables.sql`](https://github.com/Subham2S/BigData-Engineering-Capstone-Project-1/blob/main/Capstone_Inputs/HiveTables.sql)

### Spark (`.py`)

```bash
spark-submit capstone.py > /home/anabig114212/Capstone_Outputs/Cap_SparkSQL_EDA_ML.txt
```

- This [`capstone.py`](https://github.com/Subham2S/BigData-Engineering-Capstone-Project-1/blob/main/Capstone_Inputs/capstone.py) does everything. First it loads the tables and creates spark dataframes, then checks all the records again. After that Same EDA analysis is performed with the aid of sparkSQL & pySpark.
- After EDA, it checks stats for Numerical & Categorical Variables. Then proceeds towards model building after creating final df with joining the tables and dropping irrelevant columns. As per the chosen target variable 'left', the independent variables were divided into continuous and categorical variables, and in the categorical variables, two columns were label encoded manually and the rest were processed for One-Hot Encoding.
- Then, based on previous experience of EDA, both Random Forest Classification Model and Logistic Regression Model are chosen for this dataset. And as per the analysis the accuracies were 99% (RF) and 90% (LR). Model were fitted on test and train (0.3: 0.7) and gave same accuracy. Considering these as good fits, both the models were saved.
- After that a Pipeline was created and same analysis were performed in a streamlined manner to build these models. The Accuracies between the built models and the Pipeline models are very close. The reason behind the slight change in the accuracies is that the earlier case, the train & test split was performed after fitting the assembler but in case of ML pipeline, the assembler is inside the stages, so assembler is fitting on split datasets separately as a part of the pipeline. This is also clearly visible in the features column as well. So, this was a good test of the pipeline models in terms of accuracy, and we can conclude that the ML Pipeline is working properly.

### Collecting the Models

```bash
hdfs dfs -copyToLocal /user/anabig114212/random_forest.model /home/anabig114212/Capstone_Outputs/
zip -r /home/anabig114212/Capstone_Outputs/random_forest.model.zip /home/anabig114212/Capstone_Outputs/random_forest.model
rm -r /home/anabig114212/Capstone_Outputs/random_forest.model
hdfs dfs -copyToLocal /user/anabig114212/logistic_regression.model /home/anabig114212/Capstone_Outputs/
zip -r /home/anabig114212/Capstone_Outputs/logistic_regression.model.zip /home/anabig114212/Capstone_Outputs/logistic_regression.model
rm -r /home/anabig114212/Capstone_Outputs/logistic_regression.model
```
