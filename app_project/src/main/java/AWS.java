import com.amazonaws.services.s3.AmazonS3Client;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseBytes;
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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AWS {
    private final S3Client s3;
    private final SqsClient sqs;
    private final Ec2Client ec2;

    public static Region region1 = Region.US_WEST_2;
    public static Region region2 = Region.US_EAST_1;

    private static final AWS instance = new AWS();

    private AWS() {
        s3 = S3Client.builder().region(region1).build();
        sqs = SqsClient.builder().region(region1).build();
        ec2 = Ec2Client.builder().region(region2).build();
    }

    public static AWS getInstance() {
        return instance;
    }

    public String bucket_name = "giladrozsbucket";

    // S3
    public void createBucketIfNotExists(String bucket_name) {
        try {
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucket_name)
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                    .locationConstraint(BucketLocationConstraint.US_WEST_2)
                                    .build())
                    .build());
            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucket_name)
                    .build());
        } catch (S3Exception e) {
            System.out.println(e.getMessage());
        }
    }

    // S3- delete all buckets
    public void deleteBuckets() {
        for (Bucket bucket : listBuckets()) {
            deleteObjectsInBucket(bucket.name());
        }
    }

    // S3- delete a specific bucket
    public void deleteObjectsInBucket(String bucket_name) {
        try {
            ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
                    .bucket(bucket_name)
                    .build();
            ListObjectsV2Response listObjectsV2Response;

            do {
                listObjectsV2Response = s3.listObjectsV2(listObjectsV2Request);
                for (S3Object s3Object : listObjectsV2Response.contents()) {
                    DeleteObjectRequest request = DeleteObjectRequest.builder()
                            .bucket(bucket_name)
                            .key(s3Object.key())
                            .build();
                    s3.deleteObject(request);
                }
            } while (listObjectsV2Response.isTruncated());
            DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucket_name).build();
            s3.deleteBucket(deleteBucketRequest);

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    // S3 list the active buckets
    public List<Bucket> listBuckets() {
        List<Bucket> bucketList = new ArrayList<>();
        try {
            ListBucketsResponse response = s3.listBuckets();
            bucketList = response.buckets();

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return bucketList;
    }

    public boolean isThereInstanceTag(String tag) {
        List<TagDescription> tags = describeEC2Tags();
        for (TagDescription curTag : tags) {
            if (curTag.key().equals("Name") && curTag.value().equals(tag)) {
                return true;
            }
        }
        return false;
    }

    public boolean isFileInS3(String file) {
        try {
            ListObjectsRequest listObjects = ListObjectsRequest
                    .builder()
                    .bucket(bucket_name)
                    .build();

            ListObjectsResponse res = s3.listObjects(listObjects);
            List<S3Object> objects = res.contents();
            for (S3Object myValue : objects) {
                if (myValue.key().equals(file)) {
                    return true;
                }
            }
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return false;
    }

    // EC2- list the instances tags
    public List<TagDescription> describeEC2Tags() {
        List<TagDescription> tags = new ArrayList<>();
        try {
            for (Instance activeInstance : returnEC2InstancesRunning()) {
                DescribeTagsRequest tagsRequest = DescribeTagsRequest.builder()
                        .filters(
                                Filter.builder()
                                        .name("resource-id")
                                        .values(activeInstance.instanceId())
                                        .build())
                        .build();
                DescribeTagsResponse tagsResponse = ec2.describeTags(tagsRequest);
                tags.addAll(tagsResponse.tags());
            }

        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return tags;
    }

    // EC2- see details about all running instances
    public List<Instance> returnEC2InstancesRunning() {
        List<Instance> runningInstances = new ArrayList<>();
        String nextToken = null;
        try {
            do {
                DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(6).nextToken(nextToken)
                        .build();
                DescribeInstancesResponse response = ec2.describeInstances(request);
                for (Reservation reservation : response.reservations()) {
                    for (Instance instance : reservation.instances()) {
                        String instanceState = instance.state().name().toString();
                        if (!(instanceState.equals("terminated")) && !(instanceState.equals("shutting-down"))) {
                            runningInstances.add(instance);
                        }
                    }
                }
                nextToken = response.nextToken();
            } while (nextToken != null);
        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return runningInstances;
    }

    // EC2- terminate all running instances
    public void terminateAllRunningInstances() {
        List<Instance> runningInstances = returnEC2InstancesRunning();
        for (Instance instance : runningInstances) {
            String instanceId = instance.instanceId();
            terminateEC2(instanceId);
        }
    }

    // EC2- terminate an EC2 instance
    public void terminateEC2(String instanceID) {
        try {
            TerminateInstancesRequest ti = TerminateInstancesRequest.builder()
                    .instanceIds(instanceID)
                    .build();

            TerminateInstancesResponse response = ec2.terminateInstances(ti);
            List<InstanceStateChange> list = response.terminatingInstances();

        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    // S3- Insert File
    public void putInS3(String objectKey, String objectPath) {
        try {
            PutObjectRequest putOb = PutObjectRequest.builder()
                    .bucket(bucket_name)
                    .key(objectKey)
                    .build();

            s3.putObject(putOb, RequestBody.fromFile(new File(objectPath)));

        } catch (S3Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    // SQS- Create sqs
    public String createQueue(String queueName) {

        Map<QueueAttributeName, String> queueAttributes = new HashMap<>();
        queueAttributes.put(QueueAttributeName.FIFO_QUEUE, "true");
        queueAttributes.put(QueueAttributeName.CONTENT_BASED_DEDUPLICATION, "true");

        // Create a request to create the queue
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .attributes(queueAttributes)
                .build();
        try {
            CreateQueueResponse queueRes = sqs.createQueue(createQueueRequest);
            return queueRes.queueUrl();

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return "";
    }

    // SQS- send message
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
                    .messageGroupId("" + System.currentTimeMillis()) // fifo queue need a message group id , i put the
                                                                     // time stamp
                    .build());

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    // SQS- Delete all active sqs
    public void deleteActiveQueues() {
        List<String> activeQueues = listQueues();
        for (String url : activeQueues) {
            deleteSQSQueue(url);
        }
    }

    // SQS- list the active queues
    public List<String> listQueues() {
        List<String> activeSQSUrls = new ArrayList<>();
        try {
            ListQueuesRequest listQueuesRequest = ListQueuesRequest.builder().build();
            ListQueuesResponse listQueuesResponse = sqs.listQueues(listQueuesRequest);
            for (String url : listQueuesResponse.queueUrls()) {
                activeSQSUrls.add(url);
            }
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return activeSQSUrls;
    }

    // SQS- delete sqs queue
    public void deleteSQSQueue(String queueUrl) {
        try {
            DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                    .queueUrl(queueUrl)
                    .build();

            sqs.deleteQueue(deleteQueueRequest);

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public Message ReceiveFromQueue(String queueUrl) {
        List<Message> messages = new ArrayList<>();
        try {
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(1)
                    .waitTimeSeconds(5)
                    .build();

            messages = sqs.receiveMessage(receiveRequest).messages();

        } catch (QueueNameExistsException e) {
            throw e;
        }
        if (messages.size() > 0) {
            return messages.get(0);
        } else {
            return null;
        }
    }

    public void deleteFromSqsQueue(String queueUrl, String receipt) {
        DeleteMessageRequest req = DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle(receipt).build();
        sqs.deleteMessage(req);
    }

    public ResponseInputStream<GetObjectResponse> getS3ObjectFrom(String bucketName, String keyName) {
        try {
            GetObjectRequest objectRequest = GetObjectRequest
                    .builder()
                    .key(keyName)
                    .bucket(bucketName)
                    .build();
            ResponseInputStream<GetObjectResponse> output = s3.getObject(objectRequest);
            return output;
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
            return null;
        }
    }

    public String createEC2InstanceWithScript(String name, String amiId, String script) {
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(amiId)
                .instanceType(InstanceType.T2_MEDIUM)
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
            return instanceId;
        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return "";
    }
}