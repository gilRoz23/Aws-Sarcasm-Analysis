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
            aws.createSqsQueue(aws.localToManager);
            aws.createSqsQueue(aws.managerToWorker);
            aws.createSqsQueue(aws.workerToManager);
            aws.createSqsQueue(aws.managerToLocal);

            Thread listeningToLocalsThread = new Thread(new ListeningToLocals());
            Thread listeningToWorkersThread = new Thread(new ListeningToWorkers());

            listeningToLocalsThread.start();
            listeningToWorkersThread.start();

            // Wait for both threads to finish
            listeningToLocalsThread.join();
            listeningToWorkersThread.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class ListeningToLocals implements Runnable {
        public void run() {
            while (true) {
                if (!termination) {
                    aws.receivedMessagesFromQueue(aws.tasks_Queue_url);
                }
                if (aws.receivedMessagesFromLocals.isEmpty()) {
                    System.out.println("[DEBUG]: No messages in the queue tasksreceivedMessages .");
                } else {
                    Message m2 = aws.receivedMessagesFromLocals.remove(0);
                    String messageBody = m2.body();

                    if (messageBody.equals("termination")) {
                        termination = true;

                        // check counters in countersAndOutputNames map
                        Collection<Pair> countersAndOutputNames = aws.message_To_Counter_and_Output.values();
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

                    else if (messageBody.length() == 1) {
                        aws.n_from_task = Integer.parseInt(messageBody);
                    } else {
                        String[] inputParts = messageBody.split("/");
                        String localId = inputParts[0];
                        String bucketName = inputParts[1];
                        String inputName = inputParts[2];
                        aws.pullAndSend(bucketName, inputName, localId);

                    }
                }
            }
        }
    }


    static class ListeningToWorkers implements Runnable {
        @Override
        public void run() {
            while (true) {
                aws.receivedMessagesFromQueue(aws.output_Queue_url);
                if (aws.outputsListFromWorkers.isEmpty()) {
                } else {
                    aws.outputsListFromWorkers.remove(0);
                }
            }
        }
    }
}
