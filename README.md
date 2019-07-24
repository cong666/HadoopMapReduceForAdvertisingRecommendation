# HadoopMapReduceForAdvertisingRecommendation

Weight Calculation for Advertising Recommendation Using Hadoop based on HDFS

## Introduction

This program is aim to find what users like or pay more attention to according to their activities on Facebook or Twitter,
we use Hadoop 's MapReduce to speed up our calculation with massive data.


## Our Test DataSet

 - This comes from the phrases that users entered to make a search on Google, Facebook or Twitter etc.

Userid;Post  
9729784398;web player spotify mobile  
7973847384;footer locker near by  
7938783749;Nike new shoes today  
8038983998;good restaurant in paris  
8039849348;Juicer Machine Amazon  
8093800000;how to reduce your blood pressure  
0089089982;Nike store paris  

## Weight calculation
### TF*IDF
  W = TF*Log(N/DF)  
  TF,is the number of occurrences of key words for every user.  
  DF is the number of occurrences of key words in all document  
  N is the total number of lines of the document.   
  

## Architect 

### i have defined three job : Job1,Job2,Job3, each job contains one Map/Reduce calculation, these 3 jobs will be launched one by one from Job1 to Job3.
![Architect Show](https://github.com/cong666/githubimage/blob/master/screenshot_mr_shema.PNG)


##  Fully-distributed Environment

### I have created the mini distributed environment using 3 Linux Ubuntu VM (Because zookeeper need 3 machine at least)

![Environement Show](https://github.com/cong666/githubimage/blob/master/screenshot_mr_config.PNG)


## OUTPUT 
Because we have setup 4 partitions so here we have 4 file generated in the folder
![Environement Show](https://github.com/cong666/githubimage/blob/master/screenshot_mr_output1.PNG)
![Environement Show](https://github.com/cong666/githubimage/blob/master/screenshot_mr_output2.PNG)
![Environement Show](https://github.com/cong666/githubimage/blob/master/screenshot_mr_output3.PNG)

### cat these 4 files

#### OUTPUT1
ccong@node1:~/Documents/hadoop-2.9.1/bin$ ./hdfs dfs -cat /usr/file/mr/pub/output1/part-r-00000
ccong@node1:~/Documents/hadoop-2.9.1/bin$ ./hdfs dfs -cat /usr/file/mr/pub/output1/part-r-00001
ccong@node1:~/Documents/hadoop-2.9.1/bin$ ./hdfs dfs -cat /usr/file/mr/pub/output1/part-r-00002
ccong@node1:~/Documents/hadoop-2.9.1/bin$ ./hdfs dfs -cat /usr/file/mr/pub/output1/part-r-00003

- part-r-00000:    
good_8038983998	1  
juicer_8039849348	1  
machine_8039849348	1  
nike_0089089982	1  
player_9729784398	1  
restaurant_8038983998	1  
shoes_7938783749	1  
today_7938783749	1  
- part-r-00001:  
footer_7973847384	1  
how_8093800000	1  
locker_7973847384	1  
mobile_9729784398	1  
near_7973847384	1  
new_7938783749	1  
paris_0089089982	1  
spotify_9729784398	1  
- part-r-00002:  
amazon_8039849348	1  
blood_8093800000	1  
nike_7938783749	1  
paris_8038983998	1  
pressure_8093800000	1  
reduce_8093800000	1  
store_0089089982	1  
web_9729784398	1  
your_8093800000	1  
- part-r-00003:  
count	7  

#### OUTPUT2
ccong@node1:~/Documents/hadoop-2.9.1/bin$ ./hdfs dfs -cat /usr/file/mr/pub/output2/part-r-00000  

amazon	1   
blood	1  
footer	1  
good	1  
how	1  
juicer	1  
locker	1  
machine	1  
mobile	1  
near	1  
new	1  
nike	2  
paris	2  
player	1  
pressure	1  
reduce	1  
restaurant	1  
shoes	1  
spotify	1  
store	1  
today	1  
web	1  
your	1  
#### OUTPUT3-Final Result  
ccong@node1:~/Documents/hadoop-2.9.1/bin$ ./hdfs dfs -cat /usr/file/mr/pub/output3/part-r-00000  

amazon	8039849348:1.94591  
blood	8093800000:1.94591  
footer	7973847384:1.94591  
good	8038983998:1.94591  
how	8093800000:1.94591  
juicer	8039849348:1.94591  
locker	7973847384:1.94591  
machine	8039849348:1.94591  
mobile	9729784398:1.94591  
near	7973847384:1.94591  
new	7938783749:1.94591  
**nike	0089089982:1.09861**   
**nike	7938783749:1.09861**  
**paris	8038983998:1.09861**  
**paris	0089089982:1.09861**  
player	9729784398:1.94591  
pressure	8093800000:1.94591  
reduce	8093800000:1.94591  
restaurant	8038983998:1.94591  
shoes	7938783749:1.94591  
spotify	9729784398:1.94591  
store	0089089982:1.94591  
today	7938783749:1.94591  
web	9729784398:1.94591  
your	8093800000:1.94591  
