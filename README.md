# Home Work II - Map Reduce Job on DBLP Dataset
In this project work I ran the DBLP Dataset on the Apache Map/Reduce Framework to extract information such as :
* Authorship Score
* Stats on Mean Median and Max number of Co-Authors for an Author
* Range of Co-Authors for an Author
* Count for Number of Publication by an Author
* Range for number of Publication per Year

### Library Dependencies 

Listed below are different dependencies required for this porjects:

* [scala-xml] - Scala Package to parse, create XML documents
* [Scalatest.FunSuite] - Unit testing framework for the Scala
* [slf4j] - Simple Logging Facade for Java 
* [Typesafe] - to manage configuration files
* [Apache Hadoop MapReduce] - Package to manage Mappers and Reducers jobs


All these dependencies are specified in "build.sbt" and will be configured when the project is 
built.  

### Running the Code:

To run the program:
1. Clone the repo on your system
2. Create an input folder where we shall place our dblp.xml file, use the following command after running the hadoop 
```
hdfs dfs -mkdir /input
hdfs dfs -put dblp.xml /input
```
3. Create the fat jar of the the project
```
sbt clean compile assembly
```
4. Then you can run the all the Mappers and Reducers that are configured in MapReduceJob object by using following code snippet:
```
Hadoop jar HomeWork2-assembly-1.0.jar /input
```

### Solution Implemented

 The start point of the project is *MapReduce* Job, which contains Configurations for all Mappers and Reducers.
 We parse the dblp.xml file using Scala XML library and then performs various operations on the parsed data.
 Listed below are the different Mapper/Reducer Jobs implementations with description and outputs:

##### Authorship Score

+ In Mapper of the following implementation we first calculate the numbers of co-authors for a perticular publication
+ We then calculate Authorship score for each author.
+ And pass (author, authorship score) to the reducer
+ Reducer then iterates over the multiset of authorship score for an author and reduce it so a single sum value.

Output for the Authorship Score Map/Reduce job
```
Author AuthorshipScore
michael ley	1.509375
michael schwarz	0.19374961853027345
michael stonebraker	1.375
mike hamburg	0.1749755859375
mike kelley	0.1875
mohammed nadjib khenkhar	0.24609375
```
#####  Stats on Mean Median and Max number of Co-Authors for an Author
 + In Mapper for the following implementation we calculate the number of co-authors in a publication for an Author.
 + The mapper then pass the following key,value pair to reducer (author, numofCoauthors)
 + The Reducer then iterates over the values from mapper and calculates mean max and median among the co-authors for an author.
 
Output:
```
Author Max, Mean, Median
albert maier	5,4.0,4.0
alejandro p. buchmann	3,3.0,3.0
anja theuner	1,1.0,1.0
benjamin hurwitz	2,2.0,2.0
bernd walter	5,3.0,2.0
bernhard beckert	1,1.0,1.0
birgit wendholt	1,1.0,1.0
birgit wesche	1,1.0,1.0
```
##### Range of Co-Authors 
+ In Mapper for the following implementation we calculate the number of co-authors in a publication.
+ We then classify the number of co-authors we got earlier into range bins.
+ The mapper then pass the following key,value pair to reducer (range_bin, iterable(1))
+ Reducer then iterates over the multiset of range_bin values for range bins and reduce it so a single sum value.
```
Range_Co-Author Count
0	70816
1	815712
2	1284913
3	1175765
4-10	1439056
11-20	20748
121-180	2
21-40	1917
241-320	3
41-80	205
81-120	15
```
##### Range for number of Publication per Year
+ In Mapper for the following implementation we parse xml chunk to get the Year of publication for the perticular publication
+ The mapper then pass the following key,value pair to reducer (Year, iterable(1))
+ Reducer then iterates over the multiset of range_bin values for range bins and reduce it so a single sum value.
```
Year NumbersofPublication
1977	5300
1978	5802
1979	6005
1980	6975
1981	7766
1982	8871
1983	9893
1984	11597
1985	12283
1986	14421
1987	15729
```
##### Count for Number of Publication by an Author
+ In Mapper for the following implementation we parse xml chunk to get the authors for the perticular publication.
+ The mapper then pass the following key,value pair for each author of the to reducer (Author, iterable(1))
+ Reducer then iterates over the multiset of author values for range bins and reduce it so a single sum value.
```
Author NumberofPublications
erich gehlen	4
farshad nayeri	2
frank manola	8
gert smolka	    2
gisela schöpke	1
gudrun klose	1
```

### Running the Test Cases:

To run the test cases please input the following command on terminal after cloning the repo 
```
sbt clean compile test
```
The test cases runs as follows:
```
[info] MyMapperReducerTests:
[info] - Able to Load Start Tags and End Tags
[info] - The Output Folders are configured with configurations
[info] - Test for Checking Median
[info] - Test for Mean for Number of Collaborations
[info] - Test for MaxCollaboration 
[info] - Author count within the xml chunk for a particular publication 
[info] - Authorship Score for authors for a publication
[info] Run completed in 314 milliseconds.
[info] Total number of tests run: 7
[info] Suites: completed 1, aborted 0
[info] Tests: succeeded 7, failed 0, canceled 0, ignored 0, pending 0
[info] All tests passed.
[success] Total time: 4 s, completed Nov 2, 2019 2:54:06 AM
```
#### Execution Results:
+ Graph 1: In this graph we have plotted the bins for Number of CoAuthors for a Publication

X axis: Bins
Y axis: Values

![CoAuthorRange Graph](/home/karan/Documents/EMR_hw2/karan_raghani_hw2/src/main/resources/CoAuthorRange.png)

+ Graph 2: In this graph we have plotted the number of publications pulished for perticular year

X axis: Years
Y axis: Values
![YearPublication Graph](/home/karan/Documents/EMR_hw2/karan_raghani_hw2/src/main/resources/PublicationYears.png)

#### EMR Implementation
Please Refer to the embeded youtube link for details regarding deployment on AWS

[EMR deployment video](https://www.youtube.com/user/kabcdefghijkl/videos?view=0&sort=dd&shelf_id=0 "title")
