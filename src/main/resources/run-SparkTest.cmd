./bin/spark-submit \
--master <local[*] / local[2] / local / yarn / URL> \  				1* [2] two thread , [*] how many cores those many thread will start
--deploy-mode <cluster / client> \
--class in.apache.spark.SparkTest.App \
--driver-memory 4G \
--executor-memory 2G \
--executor-cores 1 \
--total-executor-cores 100 \
--num-executors 50 \
--jars <..jar, ..jar, ..jar> \
/F:/WorkSpace/Spark/SparkTest/target/SparkTest-2.4.4.jar \				***Application jar with absolute path
/F:/WorkSpace/Spark/SparkTest/src/test/resources/source/ /F:/WorkSpace/Spark/SparkTest/src/test/resources/target/ rdd	***Application arguments



1* URL(spark://207.184.161.138:7077)=Standalone URL(mesos://207.184.161.138:7077)=Mesos URL(k8s://xx.yy.zz.ww:443)=Kuberenetes