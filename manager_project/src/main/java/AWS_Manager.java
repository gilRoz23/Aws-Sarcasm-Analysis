import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.LinkedList;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.regions.Region;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Base64;

public class AWS_Manager {

    public static int Num_Of_Workers = 0;
    public String localsToManager_url;
    public String managerToWorkers_url;
    public String workersToManager_url; // worker to manager
    public String managerToLocals_url; // worker to manager

    public ExecutorService tasks_executor;
    public boolean Running;
    public List<String> workers_id = new ArrayList<>();;
    public String instance_script;
    private final S3Client s3;
    private final SqsClient sqs;
    private final Ec2Client ec2;
    private File[] files = new File[0];
    public static int currentTasksToProcess;
    public String amild = "ami-00e95a9222311e8ed";
    static String workerJar = "worker.jar";

    private static int currentWorkers = 0;

    public int n_from_task = 1;

    public String bucketNameFromLocal;

    public static Region region1 = Region.US_WEST_2;
    public static Region region2 = Region.US_EAST_1;

    private static final AWS_Manager instance = new AWS_Manager();

    private AWS_Manager() {
        s3 = S3Client.builder().region(region1).build();
        sqs = SqsClient.builder().region(region1).build();
        ec2 = Ec2Client.builder().region(region2).build();
    }

    public static AWS_Manager getInstance() {
        return instance;
    }

    public String localsToManager = "localsToManager.fifo";
    public String managerToLocals = "managerToLocals.fifo";
    public String managerToWorkers = "managerToWorkers.fifo";
    public String workersToManager = "workersToManager.fifo";

    List<Message> receivedMessagesFromLocals = new ArrayList<Message>();
    List<Message> processedSplitsFromWorkers = new ArrayList<Message>();
    private Map<String, String> keyOutputMap = new HashMap<>();
    public Map<String, Pair> localIdAndInputFileId_To_splitsToProcessCounterAndOutputFileName = new HashMap<>();

    public void initiateHashMap() {
        keyOutputMap.put("input1", "output1.txt");
        keyOutputMap.put("input2", "output2.txt");
        keyOutputMap.put("input3", "output3.txt");
        keyOutputMap.put("input4", "output4.txt");
        keyOutputMap.put("input5", "output5.txt");
    }

    public void createSqsQueue(String queueName) {
        Map<QueueAttributeName, String> queueAttributes = new HashMap<>();
        queueAttributes.put(QueueAttributeName.FIFO_QUEUE, "true");
        queueAttributes.put(QueueAttributeName.CONTENT_BASED_DEDUPLICATION, "true");

        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .attributes(queueAttributes)
                .build();
        try {
            // Create the queue
            CreateQueueResponse createQueueResponse = sqs.createQueue(createQueueRequest);
            String queueUrl = createQueueResponse.queueUrl();
            if (queueName == localsToManager) {
                localsToManager_url = queueUrl;
            } else if (queueName == workersToManager) {
                workersToManager_url = queueUrl;
            } else if (queueName == managerToLocals) {
                managerToLocals_url = queueUrl;
            } else {
                managerToWorkers_url = queueUrl;
            }
            System.out.println("Queue created: " + createQueueResponse.queueUrl());
        } catch (SqsException e) {
            System.err.println("Error creating queue: " + e.awsErrorDetails().errorMessage());
        } catch (Exception e) {
            System.err.println("Unexpected error: " + e.getMessage());
        }
    }

    public void sendMessageToQueue(String queueName, String messageBody) {
        try {
            // build a queueURl response for a url request
            GetQueueUrlResponse getQueueUrlResponse = sqs.getQueueUrl(GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build());

            String queueUrl = getQueueUrlResponse.queueUrl();

            // Send a message to the queue
            SendMessageResponse sendMessageResponse = sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(messageBody)
                    .messageGroupId("" + System.currentTimeMillis()) // fifo queue need a message group id , i putted
                                                                     // the time stamp
                    .build());

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public List<Message> getMessagesFromQAndHandle(String queueUrl) {
        try {

            ReceiveMessageResponse receiveMessageResponse = sqs.receiveMessage(ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(1) // Adjust as needed
                    .waitTimeSeconds(5) // Long polling, adjust as needed
                    .build());

            for (Message message : receiveMessageResponse.messages()) {
                if (queueUrl == localsToManager_url) {
                    receivedMessagesFromLocals.add(message);
                } else {
                    parseProcessedSplitIntoFileAndSendToS3(message);
                    // add it so fromWorkersToManager Thread know the message was sent and can
                    // delete from Q
                    processedSplitsFromWorkers.add(message);
                    //
                }
                deleteFromSqsQueue(queueUrl, message.receiptHandle());
            }
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        if (queueUrl == localsToManager_url) {
            return receivedMessagesFromLocals;
        } else {
            return processedSplitsFromWorkers;
        }
    }

    public void deleteSqsQueue(String queueName) {
        GetQueueUrlResponse getQueueUrlResponse = sqs.getQueueUrl(GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build());

        String queueUrl = getQueueUrlResponse.queueUrl();

        DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                .queueUrl(queueUrl)
                .build();

        sqs.deleteQueue(deleteQueueRequest);

    }

    public void pullFromS3SplitAndSendToWorkers(String bucketName, String fileName, String localId) {
        // we read this file once !
        bucketNameFromLocal = bucketName;
        try {
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(fileName)
                    .build();

            ResponseInputStream<GetObjectResponse> response = s3.getObject(request);

            BufferedReader reader = new BufferedReader(new InputStreamReader(response));

            List<String> splitsList = new ArrayList<>();
            String split;
            // splitting
            while ((split = reader.readLine()) != null) {
                String taggedSplit = localId + ">" + fileName + ">" + split;
                splitsList.add(taggedSplit);
            }

            response.close();
            String[] splitsArray = splitsList.toArray(new String[0]);

            // Now we have the whole input.txt file in an array.

            // Sending a message to the worker with the first line of input.txt

            // send all the messages
            for (String splitItem : splitsArray) {
                sendMessageToQueue(managerToWorkers, splitItem);
                incrementInTheMap(localId + "_" + fileName);
                launchWorkers();
            }

        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    public void deleteFromSqsQueue(String queueUrl, String receipt) {
        DeleteMessageRequest req = DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle(receipt).build();
        sqs.deleteMessage(req);
    }

    public void parseProcessedSplitIntoFileAndSendToS3(Message processedSplit) {
        String[] processedSplitParts = processedSplit.body().split("\\>");
        String local_id = processedSplitParts[0];
        String inputFile_id = processedSplitParts[1];
        String processedSplitString = processedSplitParts[2];
        String hashTable_key = local_id + "_" + inputFile_id;
        System.out.println("[DEBUG] hashTable_key: " + hashTable_key);
        try {
            OutputStream outputFilenameStream;
            String outputFileName = localIdAndInputFileId_To_splitsToProcessCounterAndOutputFileName.get(hashTable_key)
                    .getStringValue();
            outputFilenameStream = new FileOutputStream(outputFileName, true);
            PrintWriter writer = new PrintWriter(outputFilenameStream);
            writer.println(processedSplitString);
            writer.close();
            localIdAndInputFileId_To_splitsToProcessCounterAndOutputFileName.get(hashTable_key)
                    .setIntValue(localIdAndInputFileId_To_splitsToProcessCounterAndOutputFileName.get(hashTable_key)
                            .getIntValue() - 1);
            currentTasksToProcess--;
            int splitsToProcessCounter = localIdAndInputFileId_To_splitsToProcessCounterAndOutputFileName
                    .get(hashTable_key)
                    .getIntValue();
            if (splitsToProcessCounter == 0) {
                System.out.println("All the splits have been processed");
                putS3Object(outputFileName, outputFileName);
                sendMessageToQueue(managerToLocals, local_id + "/" + bucketNameFromLocal + "/" + outputFileName);
            }
        } catch (FileNotFoundException e) {
            System.out.println("[debug]: File not found");
        }
    }

    public void putS3Object(String objectKey, String objectPath) {
        try {
            PutObjectRequest putOb = PutObjectRequest.builder()
                    .bucket(bucketNameFromLocal)
                    .key(objectKey)
                    .build();

            s3.putObject(putOb, RequestBody.fromFile(new File(objectPath)));

        } catch (S3Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    public void incrementInTheMap(String inputName) {

        // Check if the key exists in the hashtable
        if (localIdAndInputFileId_To_splitsToProcessCounterAndOutputFileName.containsKey(inputName)) {
            // If the key exists, increment the integer part
            Pair existingPair = localIdAndInputFileId_To_splitsToProcessCounterAndOutputFileName.get(inputName);
            existingPair.setIntValue(existingPair.getIntValue() + 1);
        } else {
            // If the key doesn't exist, create a new pair
            Pair newPair = new Pair(1, "output_" + inputName);
            localIdAndInputFileId_To_splitsToProcessCounterAndOutputFileName.put(inputName, newPair);
            // Create a new file and add it to the files array
            File newFile = new File("output_" + inputName + ".txt");
            addFile(newFile);
        }
        currentTasksToProcess++;
    }

    // add a file to the files array
    private void addFile(File newFile) {
        File[] newFilesArray = new File[files.length + 1];
        System.arraycopy(files, 0, newFilesArray, 0, files.length);
        newFilesArray[files.length] = newFile;
        files = newFilesArray;
    }

    private void launchWorkers() {
        if (((float) currentTasksToProcess) / ((float) n_from_task) > ((float) currentWorkers) && currentWorkers <= 8) {
            String workerEC2name = "Worker"; // each time we creating a worker it will get that tag
            System.out.println("[DEBUG]: bucket name from local is " + bucketNameFromLocal);
            String workerScript = "#!/bin/bash\n" +
                    "sudo yum install -y java-1.8.0-openjdk\n" + // Install OpenJDK 8
                    "aws s3 cp s3://" + bucketNameFromLocal + "/" + workerJar + " /home/ec2-user/" + workerJar + "\n" +
                    "java -Xmx4g -jar /home/ec2-user/" + workerJar + "\n" +
                    "echo Running worker.jar...\n";
            String instanceId = createEC2Instance(workerEC2name, amild, workerScript);
            currentWorkers++;
            workers_id.add(instanceId);
            System.out.println("initialized worker");
        }
    }

    public String createEC2Instance(String name, String amiId, String script) {
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(amiId)
                .instanceType(InstanceType.T2_LARGE)
                .maxCount(1)
                .minCount(1)
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                .userData(Base64.getEncoder().encodeToString((script).getBytes()))
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);
        String instanceId = response.instances().get(0).instanceId();

        String securityGroupId = response.instances().get(0).securityGroups().get(0).groupId();
        ec2.authorizeSecurityGroupIngress(AuthorizeSecurityGroupIngressRequest.builder()
                .groupId(securityGroupId)
                .ipProtocol("tcp")
                .fromPort(22)
                .toPort(22)
                .build());

        Tag tag = Tag.builder()
                .key("Name")
                .value(name)
                .build();

        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);
            System.out.println("[DEBUG]: created an instance with tag " + name);
            return instanceId;
        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return "";
    }

    public void terminate_workers() {
        if (workers_id.size() > 0) {
            TerminateInstancesRequest terminateInstancesRequest = TerminateInstancesRequest.builder()
                    .instanceIds(workers_id)
                    .build();

            ec2.terminateInstances(terminateInstancesRequest);
        }
        System.out.println(workers_id.size() + " terminated instances");
        ec2.close();

        deleteSqsQueue(managerToWorkers);
        deleteSqsQueue(workersToManager);
        sendMessageToQueue(managerToLocals, "terminate");
        sqs.close();

        System.out.println("..................................................manager terminated safely ");
    }

}