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
