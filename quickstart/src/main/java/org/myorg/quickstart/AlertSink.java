package org.myorg.quickstart;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import java.io.File;  // Import the File class
import java.io.FileWriter;
import java.io.IOException;  // Import the IOException class to handle errors

public class AlertSink implements SinkFunction<Alert> {
    public AlertSink(){
    }
    public void invoke(Alert value,
                       SinkFunction.Context context){

        String result = null;
        // Outputting result
        if (value.getAlertPattern() == FraudulentPatterns.LOW_REACTION_TIME)
        {
            result = "Potential fraudulent action detected for user with uid : "+value.getId()+", ip : "+value.getIp()+" for ads with impression id :"+value.getImpressionId()+" with a "+value.getAlertPattern()+" pattern";
        }
        else if (value.getAlertPattern() == FraudulentPatterns.MANY_CLICKS){
            result = "Potential fraudulent action detected for user with uid : "+value.getId()+" with a "+value.getAlertPattern()+" pattern";
        }
        System.out.println(result);
        // Sinking result to a file
        // Create file if not created
        try {
            File myObj = new File("alerts.txt");
            if (myObj.createNewFile()) {
                System.out.println("File created: " + myObj.getName());
            } else {
                System.out.println("File already exists.");
            }
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        // Writing result to the file
        try {
            FileWriter myWriter = new FileWriter("alerts.txt", true);
            myWriter.write(result+"\n");
            myWriter.close();
            System.out.println("Successfully wrote alert to the file.");
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }

    }
}
