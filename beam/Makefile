run-pipeline:
	    mvn compile exec:java -Dexec.mainClass=com.peppysisay.ChatAnalysisPipeline \
	    	    -Dexec.args="--runner='DirectRunner'"
run-spark:
	    mvn compile exec:java -Dexec.mainClass=com.peppysisay.ChatAnalysisPipeline \
	    	    -Dexec.args="--runner='SparkRunner' --sparkMaster='spark://sql/protocolv1/o/4747362505192032/test-spark-cluser:443'"
install:
	    mvn install
