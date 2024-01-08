import com.amazonaws.event.ProgressEvent;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.ListQueuesResult;

import java.io.File;
import java.nio.Buffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.EnumSet;
import java.util.List;
import java.util.Scanner;

public class AmazonUtil {
    private static Scanner scan;
    private static AmazonS3 s3;
    private static AmazonSQS sqs;

    static {
        scan = new Scanner(System.in);
        s3 = AmazonS3ClientBuilder
                .standard()
                .withRegion(Regions.AP_SOUTH_1)
                .build();
        sqs = AmazonSQSClientBuilder.defaultClient();
    }
    public static void createBucket() {
        System.out.println("Create S3 Bucket");
        System.out.println("----------------");
        System.out.println();

        System.out.print("Enter bucket name (must be unique) : ");
        String bucketName = scan.nextLine();

        if(s3.doesBucketExistV2(bucketName)) {
            System.out.println("Bucket creation failed !");
            System.out.println("Bucket name already exists");
            return;
        }

        try {
            Bucket bucket = s3.createBucket(bucketName);
            System.out.println("Bucket created successfully !");
            System.out.println("Bucket name : " + bucket.getName());
        }
        catch (AmazonS3Exception exception) {
            System.out.println("Something went wrong : " + exception);
        }
    }

    public static void createQueue() {
        System.out.println("Create SQS Queue");
        System.out.println("----------------");
        System.out.println();

        System.out.print("Enter queue name : ");
        String queueName = scan.nextLine();

        try {
            CreateQueueResult queueResult = sqs.createQueue(queueName);
            System.out.println("Successfully created queue !");
            System.out.println("SQS Queue URL: " + queueResult.getQueueUrl());
        } catch (AmazonSQSException exception) {
            System.out.println("Something went wrong : " + exception.getMessage());
        }

    }

    public static void uploadFile() {
        System.out.println("Upload file to SQS Queue");
        System.out.println("------------------------");
        System.out.println();

        System.out.print("Enter bucket name     : ");
        String bucketName = scan.nextLine();
        System.out.print("Enter the file path   : ");
        String filePath = scan.nextLine();
        System.out.print("Enter the object name : ");
        String objectName = scan.nextLine();

        Path path = Paths.get(filePath);
        File file = new File(filePath);

        TransferManager tm = TransferManagerBuilder
                .standard()
                .withS3Client(s3)
                .build();

        System.out.println();
        System.out.println("Initiating upload...");
        System.out.println("--------------------");
        System.out.println();
        System.out.println("File name  : " + path.getFileName());
        System.out.println("File path  : " + filePath);
        System.out.println("Bucket name: " + bucketName);
        System.out.println("File size  : " + file.length() + " bytes");
        System.out.println();

        try {
            PutObjectRequest request = new PutObjectRequest(bucketName, objectName, file);

            request.setGeneralProgressListener(new com.amazonaws.event.ProgressListener() {
                long transferredBytes = 0;
                @Override
                public void progressChanged(ProgressEvent progressEvent) {
                    transferredBytes += progressEvent.getBytesTransferred();
                    System.out.print("\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b");
                    System.out.format("Uploaded : " + transferredBytes + " bytes ( %.4f", calculate(transferredBytes, new File(filePath).length()));
                    System.out.print("% )");
                }

            });

            Upload upload = tm.upload(request);
            upload.waitForCompletion();
            tm.shutdownNow();

            System.out.println("\nSuccessfully uploaded");
            System.out.format("Object '%s' created in the Bucket '%s'", objectName, bucketName);
            System.out.println();

        } catch (AmazonS3Exception ex) {
            System.out.println("Exception occurred : " + ex);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static double calculate(long transferredBytes, long total) {
        return (transferredBytes / (double)total) * 100;
    }

    public static void listAllBuckets() {
        System.out.println("S3 Buckets");
        System.out.println("----------");
        System.out.println();

        List<Bucket> buckets = s3.listBuckets();
        for(Bucket bucket : buckets){
            System.out.println(bucket);
        }
    }

    public static void listAllQueues() {
        System.out.println("SQS Queues");
        System.out.println("----------");
        System.out.println();

        ListQueuesResult queues = sqs.listQueues();
        for(String url : queues.getQueueUrls()) {
            System.out.println(url);
        }
    }

    public static void createNotificationOnBucket() {
        System.out.println("Create Notification on Bucket");
        System.out.println("-----------------------------");
        System.out.println();
        System.out.println("Currently available services : SQS only");
        System.out.println();

        System.out.print("Enter event name  : ");
        String eventName = scan.nextLine();
        System.out.print("Enter bucket name : ");
        String bucketName = scan.nextLine();
        System.out.print("Enter queue URL   : ");
        String queueURL = scan.nextLine();
        String queueARN = getArnFromUrl(queueURL);

        BucketNotificationConfiguration configuration = new BucketNotificationConfiguration();
        configuration.addConfiguration(eventName,
                new QueueConfiguration(queueARN, EnumSet.of(S3Event.ObjectCreated)));

        SetBucketNotificationConfigurationRequest request = new SetBucketNotificationConfigurationRequest(
                bucketName, configuration
        );

        try {
            System.out.println();
            System.out.println("Enabling event notification...");
            s3.setBucketNotificationConfiguration(request);
            System.out.println("Successfully enabled \"Object Creation\" notification on bucket");
        } catch (AmazonS3Exception exception) {
            System.out.println("Something went wrong : " + exception);
        }
    }

    private static String getArnFromUrl(String url) {
        StringBuilder arn = new StringBuilder("arn:aws:");
        String[] urlParts = url.split("/");

        String[] serviceAndRegion = urlParts[2].split("\\.");
        String service = serviceAndRegion[0];
        String region = serviceAndRegion[1];

        arn.append(service);
        arn.append(":");
        arn.append(region);
        arn.append(":");
        arn.append(urlParts[3]); //account ID
        arn.append(":");
        arn.append(urlParts[4]); //queue name

        return arn.toString();
    }
}
