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
        aws.createSQS(aws.managerToWorkers);
        aws.createSQS(aws.WorkersToManager);
        getSplitAndProcessAndSendToManager();
    }

    public static void getSplitAndProcessAndSendToManager() {
        while (true) {
            Message msg = extractMessageFromQ();
            if (msg != null) {
                String body = msg.body();
                String[] messageParts = body.split("\\>");
                String localId = messageParts[0];
                String inputId = messageParts[1];
                String split = messageParts[2];

                // kind of a record reader, splitting the big split to little input units for
                // the wrokers
                ReviewsStructure reviewsStructure = ReviewsStructure.StringToReviewsStructure(split);
                StanfordActivator reviewMapper = new StanfordActivator();
                String processedSplitString = "";
                ProcessedReviewsStructure processedReviewsStructuer = new ProcessedReviewsStructure();
                for (Review review : reviewsStructure.getReviews()) {
                    processedReviewsStructuer.appendProcessedReview(reviewMapper.process(review));
                }

                processedSplitString = processedReviewsStructuer.processedReviewsToJSONString();
                // you can now delete split from Q
                aws.deleteFromSQS(aws.managerToWorkers_url, msg.receiptHandle());
                // 
                aws.SendMessageToManager(localId + ">" + inputId + ">" + processedSplitString);
            }

        }

    }

    private static Message extractMessageFromQ() {
        aws.receivedMessagesFromQueue(aws.managerToWorkers_url);
        if (aws.reviewsReceivedMessages.isEmpty()) {
            return null;
        }
        Message m = aws.reviewsReceivedMessages.remove(0);
        return m;
    }

}
