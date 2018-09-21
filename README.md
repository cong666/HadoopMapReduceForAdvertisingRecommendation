# HadoopMapReduceForAdvertisingRecommendation

Weight Calculation for Advertising Recommendation Using Hadoop based on HDFS

## Introduction

This program is aim to find what users like or pay more attention to according to their activities on Facebook or Twitter,
we use Hadoop 's MapReduce to speed up our calculation with massive data.


## Test DataSet

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

  W = TF*Log(N/DF)  
  TF,is the number of occurrences of key words for every user.  
  DF is the number of occurrences of key words in all document  
  N is the total number of lines of the document.   
  

## Architect 

### i have defined three job : Job1,Job2,Job3, each job contains one Map/Reduce calculation, these 3 jobs will be launched one by one from Job1 to Job3.
![Architect Show](https://github.com/cong666/githubimage/blob/master/ 	screenshot_mr_shema.PNG)


##  Fully-distributed Environment

### I have created the mini distributed environment using 3 Linux Ubuntu VM

![Environement Show](https://github.com/cong666/githubimage/blob/master/screenshot_mr_config.png)
