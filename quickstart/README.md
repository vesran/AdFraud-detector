A Flink application project using Java and Gradle.

To package your job for submission to Flink, use: 'gradle shadowJar'. Afterwards, you'll find the
jar to use in the 'build/libs' folder.

To run and test your application with an embedded instance of Flink use: 'gradle run'

# Observability

### Get your environment running 

    From now, we suppose that you have set up kafka-zk containers and add events to the topics it contains

1. Install ElasticSearch and Kibana with docker - [docker](https://www.elastic.co/guide/en/elasticsearch/reference/current/docker.html) / [kibana](https://www.elastic.co/guide/en/kibana/current/docker.html) 

2. Launch your containers following the instructions in the previous links (If you specify another port for any reasons, make sure that the ports are the same as the one provided in the flink files)

3. Launch the StreamingJob main method (for Scala) | Launch the ActivityConsumer main method (for Java)

### See some dashboards for analysis

4. Go to Kibana UI : [http://localhost:5601](http://localhost:5601)

5. Add an index pattern (*Tap "Index pattern in the search bar*) corresponding to `displays-clicks-idx` (for Scala) | `clickdisplayidx` (for java)

6. Go to dashboards (*Click on the toggle on the left side) to add and visualize your own dashboards 
7. Select : *Create New panel* and click on *Lens* to start do some analysis

#### Example of obtainable dashboards - 
---

**Total number of click in proportion of all events**
![alt text](./data/CTR.png)

---
**Traffic over a period of time**
![alt text](./data/Traffic.png)

---
**Nature of events (Click and displays) per users (uid)**
![alt text](./data/CTRUser.png)

---
