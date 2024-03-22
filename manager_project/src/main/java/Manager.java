import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import java.util.Collection;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

public class Manager {
    final static AWS_Manager aws = AWS_Manager.getInstance();
    static boolean termination = false;

    public static void main(String[] args) {
        try {
            aws.createSqsQueue(aws.localsToManager);
            aws.createSqsQueue(aws.managerToLocals);
            aws.createSqsQueue(aws.managerToWorkers);
            aws.createSqsQueue(aws.workersToManager);

            Thread fromLocalsToWorkersThread = new Thread(new fromLocalsToWorkers());
            Thread fromWorkersToLocalsThread = new Thread(new fromWorkersToLocals());

            fromLocalsToWorkersThread.start();
            fromWorkersToLocalsThread.start();

            // Wait for both threads to finish
            fromLocalsToWorkersThread.join();
            fromWorkersToLocalsThread.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class fromLocalsToWorkers implements Runnable {
        public void run() {
            while (true) {
                if (!termination) {
                    aws.getMessagesFromQAndHandle(aws.localsToManager_url);
                }
                if (aws.receivedMessagesFromLocals.isEmpty()) {
                    System.out.println("[DEBUG]: No messages in the queue localsToManager .");
                } else {
                    Message queueMessage = aws.receivedMessagesFromLocals.remove(0);
                    String taggedMessageFromLocal = queueMessage.body();

                    if (taggedMessageFromLocal.equals("termination")) {
                        termination = true;

                        // check counters in countersAndOutputNames map
                        Collection<Pair> countersAndOutputNames = aws.localIdAndInputFileId_To_splitsToProcessCounterAndOutputFileName
                                .values();
                        while (countersAndOutputNames.stream()
                                .anyMatch(counterAndOutputName -> counterAndOutputName.intValue > 0)) {
                            System.out.println("[DEBUG]: manager sleep for 0.6 second waiting for all workers to done");
                            try {
                                Thread.sleep(600);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }

                        System.out.println("[DEBUG]: manager terminating the workers");
                        aws.terminate_workers();
                    }

                    else if (taggedMessageFromLocal.length() == 1) {
                        aws.n_from_task = Integer.parseInt(taggedMessageFromLocal);
                    } else {
                        String[] inputParts = taggedMessageFromLocal.split("/");
                        String localId = inputParts[0];
                        String bucketName = inputParts[1];
                        String inputName = inputParts[2];
                        aws.pullFromS3SplitAndSendToWorkers(bucketName, inputName, localId);

                    }
                }
            }
        }
    }

    static class fromWorkersToLocals implements Runnable {
        @Override
        public void run() {
            while (true) {
                aws.getMessagesFromQAndHandle(aws.workersToManager_url);
                if (aws.processedSplitsFromWorkers.isEmpty()) {
                } else {
                    // can now delete the message from the Q
                    aws.processedSplitsFromWorkers.remove(0);
                    //
                }
            }
        }
    }
}
