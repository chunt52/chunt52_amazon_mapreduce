# chunt52_amazon_mapreduce
Java code for map reduce on dsba-hadoop.uncc.edu.  There are 14,741,571 amazon products and 153,871,242 amazon product reviews.

# gson JavaDoc
https://www.javadoc.io/doc/com.google.code.gson/gson/latest/com.google.gson/com/google/gson/JsonObject.html

# Test Harness
You can test a small snipped of code (e.g. JSON parsing and manipulation) using jdoole.com.
1. Go to https://www.jdoodle.com/online-java-compiler/
2. Click on the `...` button after the `Execute` button.  Then select "*External Libraries (from Maven repo)*" and paste in `com.google.code.gson:gson:2.8.6`
3. Paste the code from `test_harness.java` into the text window and select `Execute`

# Executing AmazonContainsReview
**Be sure to replace "chunt52" with your own user name below!**
1. Log into dsba-hadoop.uncc.edu using ssh
2. `git clone https://github.com/chunt52-edu/chunt52_amazon_mapreduce.git` to clone this repo
3. Go into the repo directory.  In this case: `cd chunt52_amazon_mapreduce`
4. Make a "build" directory (if it does not already exist): `mkdir build`
5. Compile the java code (all one line).  You may see some warnings--that' ok. 
`javac -cp /opt/cloudera/parcels/CDH/lib/hadoop/client/*:/opt/cloudera/parcels/CDH/lib/hbase/* AmazonContainsReview.java -d build -Xlint`
6. Now we wrap up our code into a Java "jar" file: `jar -cvf process_ratings.jar -C build/ .`
7. This is the final step  
 - Note that you will need to delete the output folder if it already exists otherwise you will get an "Exception in thread "main" org.apache.hadoop.mapred.FileAlreadyExistsException: Output directory hdfs://dsba-nameservice/user/... type of error: 
   - `hadoop fs -rm -r /user/chunt52/review_ratings`
   - `hadoop fs -rm -r /user/chunt52/review_diff` 
   - `hadoop fs -rm -r /user/chunt52/review_bins`
 - Now we execute the map-reduce job: `HADOOP_CLASSPATH=$(hbase mapredcp):/etc/hbase/conf hadoop jar process_ratings.jar AmazonContainsReview '/user/chunt52/review_ratings' '/user/chunt52/review_diff' '/user/chunt52/review_bins'`
 - Once that job completes, you can concatenate the outputs across all output files with: 
   - `hadoop fs -cat /user/chunt52/review_ratings/*` 
   - `hadoop fs -cat /user/chunt52/review_diff/*`
   - `hadoop fs -cat /user/chunt52/review_bins/*`
 - Or if you have output that is too big for displaying on the terminal screen you can do to redirect all output:
   - `hadoop fs -cat /user/chunt52/review_ratings/* > output.txt` 
   - `hadoop fs -cat /user/chunt52/review_diff/* > output2.txt` 
   - `hadoop fs -cat /user/chunt52/review_bins/* > output3.txt`
 - Copying the output to your local machine can be done with `scp` (for windows/putty `pscp -P 22 chunt52@dsba-hadoop.uncc.edu:/users/chunt52/dsba-6190/test.txt .`)


 This code compares the reviews within a item containing the words "reviews" or "comments" with the total average rating of that item and displays the difference in bins. The output is:
| difference |  count  |
|:----------:|:-------:|
|    3<x≤5   | 2741075 |
|    0<x≤2   |  913967 |
|    2<x≤3   |  167839 |