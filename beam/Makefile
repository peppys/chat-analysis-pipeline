run-pipeline:
	    mvn compile exec:java -Dexec.mainClass=com.peppysisay.ChatAnalysisPipeline \
	        -Dexec.args="--runner='DirectRunner'"
run-dataflow:
	    mvn compile exec:java -Dexec.mainClass=com.peppysisay.ChatAnalysisPipeline \
	        -Dexec.args="--runner='DataflowRunner'"
install:
	    mvn install
