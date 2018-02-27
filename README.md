# stream-ingest-example

reads from pubsub and writes to BQ.

TO RUN

1.
  mvn clean install 

2.
  mvn compile exec:java -Dexec.mainClass=className -Dexec.args="--project=[] --stagingLocation=[] --streaming=true  --dataset=[] --runner=[] --subscriptionName=[] --tablename=[]"
