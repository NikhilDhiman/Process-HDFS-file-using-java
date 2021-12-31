# Process HDFS file using java and store aggregate data in MySQL

The Data (CSV file) is stored on Hadoop Distributed File System (HDFS).
The first step is to read the data from HDFS using JAVA and then, the file got processed in java with certain operations as calculating median of numeric column and mode of string columns. After successfully processing the file, the aggregated results are stored on MySQL table.

# Environment Used:
1. Hadoop version : 2.10.1
2. Java Version: 1.8.0
3. Eclipse Version: 2019-12(4.14.0)
4. Spark Version: 2.4.7

The dummy data of users has the columns as name, age, city, country (in the csv format). 

The path of data on HDFS is: /inputRawData/userData.csv

DataBase Details:
Database Name: processedData
Table Name: aggregateUserData

# Command to submit the job on spark cluster:
spark-submit --driver-class-path /home/nikhil/Downloads/mysql-connector-java-8.0.23.jar hdfs-java-mysql.jar
