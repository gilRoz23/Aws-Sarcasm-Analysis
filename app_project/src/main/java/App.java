import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.sqs.model.Message;

import com.amazonaws.util.Base64;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class App {
    final static AWS aws = AWS.getInstance();
    static String[] inputFilePaths;
    static String[] outputFilePaths;
    static int n;
    static String queueLocalAndManager;
    static String inputQueueName = "localToManager.fifo";
    static String outputQueueURL;
    static String outputQueueName = "managerToLocal.fifo";
    static int processedMessages = 0;
    static String amild = "ami-00e95a9222311e8ed";
    static String myId;
    static String managerJar = "manager.jar";
    static String workerJar = "worker.jar";
    private static boolean toTerminate = false;

    public static void main(String[] args) {
        try {
            boolean onlyTerminate = false;
            if (onlyTerminate){
                termination();
            }
                processArgs(args);
                setupAWS();
                waitForManager();
                if(toTerminate)
                    termination();
                System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static void createManagerIfNotExistsWithScript() {
        String managerScript = "#!/bin/bash\n" +
        "sudo yum makecache fast\n" + 
        "sudo yum install -y java-1.8.0-openjdk\n" + 
        "aws s3 cp s3://" + aws.bucket_name + "/" + managerJar + " /home/ec2-user/" + managerJar + "\n" +
        "java -jar /home/ec2-user/" + managerJar + "\n" +
        "echo Running Manager.jar...\n";    
        if (!aws.checkForInstanceTag("Manager")) {
            aws.putS3Object(managerJar, managerJar);
            aws.putS3Object(workerJar, workerJar);
            while (!aws.checkForFile(managerJar)) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            aws.createEC2InstanceWithScript("Manager", amild, managerScript);

        }
    }


    // Create Buckets, Create Queues, Upload JARs to S3
    private static void setupAWS() {
        myId = IdGenerator.getId();
        aws.createBucketIfNotExists(aws.bucket_name);
        createManagerIfNotExistsWithScript();
        aws.createQueue(inputQueueName);
        outputQueueURL = aws.createQueue(outputQueueName);
        aws.sendMessageToQueue(inputQueueName, "" + n);
        for (String file : inputFilePaths) {
            aws.putS3Object(file, file);
            aws.sendMessageToQueue(inputQueueName, myId + "/" + aws.bucket_name + "/" + file);
        }
        if(toTerminate)
            aws.sendMessageToQueue(inputQueueName, "termination");
        
    }


    private static void processArgs(String[] args) {
        int argsLen = args.length;
        if (argsLen < 3) {
            System.err.println("Error: Insufficient arguments. Please provide at least 3 args: input, output and n.");
            System.exit(1); // Exit with a non-zero status code indicating an error
        }
        if (argsLen % 2 == 0 && args[argsLen - 1].equals("terminate")) {
            toTerminate = true;
            argsLen = argsLen - 1;
        }
        n = Integer.parseInt(args[argsLen - 1]);
        argsLen = argsLen - 1;
        int numOfFiles = argsLen / 2;
        inputFilePaths = new String[numOfFiles];
        System.arraycopy(args, 0, inputFilePaths, 0, numOfFiles);
        outputFilePaths = new String[numOfFiles];
        System.arraycopy(args, numOfFiles, outputFilePaths, 0, numOfFiles);
    }

    private static void termination() {
        aws.terminateAllRunningInstances();
        aws.deleteBuckets();
        aws.deleteActiveQueues();
    }


    private static void waitForManager() {
        int expectedLen = inputFilePaths.length;
        Message ls;
        while (processedMessages < expectedLen) {
            ls = aws.ReceiveFromQueue(outputQueueURL);
            if (ls == null) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                String[] parts = ls.body().split("/");
                if (parts[0].equals(myId)) {
                    processMessages(ls, parts[1], parts[2]);
                }

            }
        }
    }

    public static void processMessages(Message ls, String bucketName, String key) {
        createHTML(bucketName, key);
        processedMessages += 1;
        aws.deleteFromSqsQueue(outputQueueURL, ls.receiptHandle());
    }

    public static void createHTML(String bucketName, String fileName) {
        try {
            ResponseInputStream<GetObjectResponse> temp = aws.getS3ObjectFrom(bucketName, fileName);
            InputStream is = temp;
            FileOutputStream outputStream = new FileOutputStream(outputFilePaths[processedMessages]);
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(is));
            PrintWriter writer = new PrintWriter(outputStream);
            writer.println("<!DOCTYPE html>");
            writer.println("<html>");
            writer.println("<body>");
            String[] towrite;
            String curJson;
            while ((curJson = reader.readLine()) != null) {
                ObjectMapper objectMapper = new ObjectMapper();
                try {
                    JsonNode jsonNode = objectMapper.readTree(curJson);
                    JsonNode processedEntries = jsonNode.get("processedEntries");
                    if (processedEntries.isArray()) {
                        for (JsonNode entry : processedEntries) {
                            boolean isSarcastic = entry.get("isSarcastic").asBoolean();
                            String color = entry.get("color").asText();
                            String link = entry.get("link").asText();
                            JsonNode namedEntitiesNode = entry.get("namedEntities");
                            writer.println("<div>");
                            writer.println("<p>Link: <a style=\"color:" + color + ";\" href=\"" + link + "\">" + link
                                    + "</a></p>");
                            writer.println("<p>Named Entities: " + namedEntitiesNode + "</p>");
                            writer.println("<p>Sarcasm Detection: " + (isSarcastic ? "Sarcastic" : "Not Sarcastic")
                                    + "</p>");
                            writer.println("</div>");
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            writer.println("</body>");
            writer.println("</html>");
            is.close();
            reader.close();
            writer.close();
        } catch (FileNotFoundException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }
}