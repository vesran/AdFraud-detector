A Flink application project using Java and Gradle.

To package your job for submission to Flink, use: 'gradle shadowJar'. Afterwards, you'll find the
jar to use in the 'build/libs' folder.

To run and test your application with an embedded instance of Flink use: 'gradle run'

## Get your environment running 

1. Install ElasticSearch and Kibana with docker - [docker](https://www.elastic.co/guide/en/elasticsearch/reference/current/docker.html) / [kibana](https://www.elastic.co/guide/en/kibana/current/docker.html) 

2. Launch your containers following the instructions in the previous links (If you specify another port for any reasons, make sure that the ports are the same as the one provided in the flink files)

3. Launch the StreamingJob main method (for Scala)

## Observability

4. Go to Kibana UI : [http://localhost:5601](http://localhost:5601)

5. Add an index pattern corresponding to `displays-clicks-idx`

6. Go to dashboards to add and visualize your own dashboards