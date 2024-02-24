import java.util.*;

import software.amazon.awssdk.services.ec2.model.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityResponse;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

public class Worker {
    final static AWS_Worker aws = AWS_Worker.getInstance();
    public static void main(String[] args) {
        aws.createSQS(aws.managerToWorkerQ);
        aws.createSQS(aws.WorkerToManagerQ);
        parsingLoop();
    }

    public static void parsingLoop(){        
        while(true){
            Message msg = workOnMessage();
            if (msg != null){
                String body = msg.body();
                String []messageParts = body.split("\\>");
                String localId = messageParts[0];
                String inputId = messageParts[1];
                String line = messageParts[2];

                
                Input input = Input.StringToInput(line);
                StanfordActivator reviewMapper = new StanfordActivator();
                String MessageToSqs = "";
                Output outputManager = new Output();
                for(Review review : input.getReviews()) {
                    outputManager.appendProcessedReview(reviewMapper.process(review));
                }


                MessageToSqs = outputManager.OutputToJSONString();
                aws.deleteFromSQS(aws.reviewsURl, msg.receiptHandle());
                aws.SendMessageToManager(localId + ">" + inputId + ">" + MessageToSqs);
            }

    }

    }

    private static Message workOnMessage() {
        aws.receivedMessagesFromQueue(aws.reviewsURl);
        if (aws.reviewsReceivedMessages.isEmpty()) {
            return null;
        }
        Message m = aws.reviewsReceivedMessages.remove(0);
        return m;
    }

}
