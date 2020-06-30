# SparkFinalAssignment

MOdelgen.scala - This consists of model generation

Modelpred.scala - This consists of predicting the test.csv using the model generated above

ModelStream.scala - This consists of predicting Streaming data

*********************************************************************************************************************************************************************
Run Instructions:
1.Generating the Model using Training data
spark2-submit --class "ModelGenerator.Modelgen" --master yarn /mnt/home/edureka_893377/Spark/BicycleProject.jar /user/edureka_893377/Input/Spark/Final/train.csv /user/edureka_893377/Input/Spark/Final/Model

2.Predicting the outcome using test data on the Model generated
spark2-submit --class "ModelPrediction.Modelpred" --master yarn /mnt/home/edureka_893377/Spark/BicycleProject.jar /user/edureka_893377/Input/Spark/Final/test.csv /user/edureka_893377/Input/Spark/Final/Model /user/edureka_893377/Input/Spark/Final/Output

3.Starting flume agent
flume-ng agent --conf conf --conf-file ~/Spark/my_flume_agent --name myagent -Dflume.root.logger=INFO,console

4.Send data to HDFS location(/user/edureka_893377/Input/Spark/Final/FlumeHDFS) using Flume
telnet localhost 44444

5. Doing prediction on Streaming data
spark2-submit  --jars /mnt/home/edureka_893377/Spark/com.mysql.jdbc_5.1.5.jar --class "ModelStreaming.ModelStream" --master yarn /mnt/home/edureka_893377/Spark/BicycleProject.jar
