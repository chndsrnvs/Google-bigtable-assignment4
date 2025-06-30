# Google-bigtable-assignment4

Assignment-4
Google Big Table: 50 points
Submit a PDF with code listing, and screenshots showing outputs of insert(), delete(), and the
queries. Screenshots should be uniquely distinguishable for each submission. Be careful of
plagiarism from online sources/peers.
Connecting to the Instance after setting it up as explained in class.
Connecting to the database can be done using the cbt command-line tool or using a Bigtable
client library. Google Cloud Bigtable is not a relational database and is NOT accessible using
SQuirreL or other SQL tools.
Accessing using cbt command-line tool
The cbt command-line interface allows performing basic administrative tasks and
reading/writing data from tables. There is a tutorial on cbt CLI found here:
https://cloud.google.com/bigtable/docs/create-instance-write-data-cbt-cli?_ga=2.111890764.-9
13511634.1664467746
Accessing using Client Library
The lab will use the Java client library. An example code file called HelloWorld.java shown in
class. This sample creates a table, writes data, reads data, then deletes the table. There is more
information on this "Hello world" example. Found here:
https://cloud.google.com/bigtable/docs/samples-java-hello-world
For setup, follow these instructions. From here:
https://cloud.google.com/docs/authentication/provide-credentials-adc
You will need to install the Google Cloud CLI then run the command:
gcloud auth application-default login.
In the given starter code, fill the functions marked as TODO
1. 10 mark - Write the method connect() to create a connection. Create a Bigtable data
client and admin client. See HelloWorld.java for starter code.
2. 10 mark - Write the method createTable() to create a table to store the sensor data.
3. 5 marks - Write the method load() to load the sensor data into the database. The data
files are in the data folder.
4. 10 marks - Write the method query1() that returns the temperature at Vancouver on
2022-10-01 at 10 a.m.
5. 5 marks - Write the method query2() that returns the highest wind speed in the month
of September 2022 in Portland.
6. 5 marks - Write the method query3() that returns all the readings for SeaTac for October
2, 2022.
7. 5 marks - Write the method query4() that returns the highest temperature at any station
in the summer months of 2022 (July (7), August (8)).
