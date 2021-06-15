# Apache-Beam-Training

## Data processing job that produces statistics related to some events.

### To run this job locally you need to add the next command line arguments:
1) --input (where to read initial data from)
2) --output (where to write resulting data)
3) --runner (DirectRunner.class for local running)
4) --debugMode (flag that signals whether to run the pipeline in debug mode, useful to write some mediate data)

#### If you want to add some custom options - modify JobOptions interface. 
#### If you want to run the pipeline on some other environment (Cloud Dataflow, Spark, Flink etc.) - check Beam SDK for the desired runners and update the runner profile in pom.xml.
#### In order to have a proper view of .avro files - install Avro/Parquet Viewer plugin for IntelliJ.
#### P.S there is also a sample processing job (sample/ directory), which aggregates event data and writes output to .txt files, nevermind on it :)