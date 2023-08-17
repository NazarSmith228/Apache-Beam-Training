# Apache-Beam-Training

## EventAggregator Job

### EventAggregator is a data processing job that produces statistics related to some events.
#### P.S there is also a sample processing job (other/ directory), which aggregates event data and writes output to .txt files, nevermind on it :)

## WordAggregator Job
### Word aggregator is a word-processing job.

### To run any of those jobs locally you need to add the next command line arguments:
#### Mandatory:
#### 1) --input (where to read initial data from)
#### 2) --output (where to write resulting data)

#### Optional:
#### 1) --runner (DirectRunner.class for local running)
#### 2) --debugMode (flag that signals whether to run the pipeline in debug mode, useful to write some mediate data)
#### 3) --debugPath(where to write mediate data)
#### 4) --schemaPath(where to write a generated AVRO schema. Schema generation is used in some jobs)

### If you want to add some custom options - modify .*Options interface (such interfaces are available under /config folder). 
### If you want to run the pipeline on some other environment (Cloud Dataflow, Spark, Flink etc.) - check Beam SDK for the desired runners and update the runner profile in pom.xml.
### In order to have a proper view of .avro files - install Avro/Parquet Viewer plugin for IntelliJ.
