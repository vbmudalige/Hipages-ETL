
## Instructions
ETLApp.scala is the starting point of the application. When the run method is executed, the extract, transform and load functions will run sequentially.
In order to run the app, just execute: **sbt "run events_json_path output_path json_schema_path"**. Schema path should be relative to the resources folder.

## Extraction
Input Data Source Factory should be implemented. We can pass the required data source name and the associated parameters as command line arguments when executing the jar. Extracting class will get the corresponding data source object from the factory using the data source name. Then data source object will be called with the parameters(eg: if it is a distributed file system => file path and the type. If it is a message broker =>  topic and the ip). The data source will return a dataframe which includes the events. I will use the  .option("mode", "DROPMALFORMED") to avoid corrupted records. --conf spark.sql.files.ignoreCorruptFiles=true parameter is used when submitting the spark job to avoid corrupted files. Logger should be configured and set to info level to detect all the the dropped records and ignored files. I prefer to eliminate all the corrupted data at the extraction step because it simplifies a lot of things at the transformation stage. When we extracting structured data, it should be validated using the schema. If there are more mismatches than a predefined threshold value, the extraction should be failed because it may be a sign of a schema change. Schema comparison need to be done in such a situation before running the ETL. 

## Transformation
In this implementation I have used hard coded transformations to process the events due to limited time. But if we are taking this to production this transformations should happen in a dynamic manner. I should implement a transformation pipe by including all the possible fields in the event schema. This transformation pipe should be called with the required field names and the extracted input data frame. We will get a transformed dataframe as the output which includes all the required fields. All the field names should be moved to a separate property file to enhance the maintenance. 
 
## Loading
Output data source factory should be implemented as similar to input data source factory. The output destination can be anything from S3, GS bucket, HDFS to BigTable. The output datasource factory should provide the suitable output datasource object based on the output destination and the file type. The file type, destination and the corresponding parameters such as partitioning column names should be provided as command line argumentss. 

## Other notable things 
All the command line parameters should be validated using a library like ScaldingArgs. Configurations should be maintained for each of the environment separately(Dev, Stress, UAT, Prod). A workflow scheduler like Azkaban or Airflow can be used to execute this job periodically. Scripts should be provided to generate the workflow configs.

## Testing
Unit tests need to be added to test the functionalities of the transformation logic. All the edge cases, null and empty values should be tested in the unit tests. After completing the development including all the unit tests, the job should be deployed and run in the QA environment. QA testing need to be done to validate the features list. Stress deployment and testing need to be done to validate the ability of handling high load. Also, an integration test should be done to verify that the job is working properly with the platform and the rest of the services. Then the UAT deployment need to be done and job should be scheduled and run as similar to production environment. After passing all of these steps, the job can be deployed in the production environment. All the documents including test results, run books, deployment plans and DRs need to be prepared before the production deployment.

## Portability and Scaling
I have implemented this job using Apache Spark framework therefore it will be really easy to scale horizontally by just increasing the number of executors. Spark is 100 times faster than Hadoop MapReduce. Spark is Polyglot and can be integrated with Hadoop ecosystem very easily. Therefore Spark is a great choice for this sort of batch processing use case. Spark jobs can be run in a YARN cluster or in a cloud service like Dataproc or EMR very easily and therefore it can be considered as a portable solution. 

### The last commit before the deadline
https://github.com/vbmudalige/Hipages-ETL/commit/cf1b20f299b67c2349cfafc96e4ca69f050fe5f7