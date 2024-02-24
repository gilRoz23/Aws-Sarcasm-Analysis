import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.Base64;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

public class AWS_Worker {

    private final S3Client s3;
    private final SqsClient sqs;
    private final Ec2Client ec2;

    public String reviewsURl;
    public String outputURL;


    public static Region region1 = Region.US_WEST_2;
    public static Region region2 = Region.US_EAST_1;
    
    private static final AWS_Worker instance = new AWS_Worker();

    private AWS_Worker() {
        s3 = S3Client.builder().region(region1).build();
        sqs = SqsClient.builder().region(region1).build();
        ec2 = Ec2Client.builder().region(region2).build();
    }

    public static AWS_Worker getInstance() {
        return instance;
    }

    public  String managerToWorkerQ = "managerToWorker.fifo";
    public  String WorkerToManagerQ = "workerToManager.fifo";

    List<Message> reviewsReceivedMessages = new ArrayList<Message>();
    List<Message> outputSendMessages = new ArrayList<Message>();

    
    public void createSQS(String queueName) {
        Map<QueueAttributeName, String> queueAttributes = new HashMap<>();
        queueAttributes.put(QueueAttributeName.FIFO_QUEUE, "true");
        queueAttributes.put(QueueAttributeName.CONTENT_BASED_DEDUPLICATION, "true");
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .attributes(queueAttributes)
                .build();
        try {
            CreateQueueResponse createQueueResponse = sqs.createQueue(createQueueRequest);
            String queueUrl = createQueueResponse.queueUrl();
            if(queueName == WorkerToManagerQ){
                outputURL = queueUrl;
                System.out.println("tasks url " + outputURL);
            }
            else{
                reviewsURl = queueUrl;
            }
            System.out.println("Queue created: " + createQueueResponse.queueUrl());
        } catch (SqsException e) {
            System.err.println("Error creating queue: " + e.awsErrorDetails().errorMessage());
        } catch (Exception e) {
            System.err.println("Unexpected error: " + e.getMessage());
        }
    }
    

    public List<Message> receivedMessagesFromQueue(String queueUrl) {
         try {
            // Receive messages from the queue
            ReceiveMessageResponse receiveMessageResponse = sqs.receiveMessage(ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(1) 
                    .waitTimeSeconds(5) 
                    .visibilityTimeout(600) 
                    .build());
            
            if(receiveMessageResponse.hasMessages()){
                for (Message message : receiveMessageResponse.messages()) {
                    System.out.println("Received Message:");
                    System.out.println("  MessageId: " + message.messageId());
                    System.out.println("  ReceiptHandle: " + message.receiptHandle());
                    System.out.println("url " +queueUrl);
                    reviewsReceivedMessages.add(message);
            }


            }
        }catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return reviewsReceivedMessages;
    }
    public  void SendMessageToManager(String message){
        sendMessageToQueue(WorkerToManagerQ, message);
    
    }


    public void sendMessageToQueue(String queueName, String messageBody) {
        try{
            // build a queueURl response for a url request
            GetQueueUrlResponse getQueueUrlResponse = sqs.getQueueUrl(GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build());

            String queueUrl = getQueueUrlResponse.queueUrl();
           

            // Send a message to the queue
            SendMessageResponse sendMessageResponse = sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(messageBody)
                    .messageGroupId(""+System.currentTimeMillis()) 
                    .build());

            System.out.println("[DEBUG]: Message sent! MessageId: " + sendMessageResponse.messageId());

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }


    public void deleteFromSQS(String queueUrl, String receipt){
        DeleteMessageRequest req = DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle(receipt).build();
        sqs.deleteMessage(req);
    }


}

