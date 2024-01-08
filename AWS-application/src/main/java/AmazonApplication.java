import java.util.Scanner;

public class AmazonApplication {
    public static Scanner scan = new Scanner(System.in);
    public static void main(String[] args) throws InterruptedException {
        loop : while(true) {
            System.out.println();
            System.out.println("Welcome to Amazon Web Services");
            System.out.println("------------------------------");
            System.out.println();

            System.out.println("1. Create a S3 Bucket");
            System.out.println("2. Create a SQS Queue");
            System.out.println("3. Upload file to S3 Bucket");
            System.out.println("4. List all S3 Buckets");
            System.out.println("5. List all SQS Queues");
            System.out.println("6. Create Notification on Bucket");
            System.out.println("Exit");
            System.out.println();

            System.out.print("Choose any one service : ");
            int option = Integer.valueOf(scan.nextLine());
            Liner.endLine();

            switch (option) {
                case 1 ->
                    AmazonUtil.createBucket();
                case 2 ->
                    AmazonUtil.createQueue();
                case 3 ->
                    AmazonUtil.uploadFile();
                case 4 ->
                    AmazonUtil.listAllBuckets();
                case 5 ->
                    AmazonUtil.listAllQueues();
                case 6 ->
                    AmazonUtil.createNotificationOnBucket();
                default -> {
                    break loop;
                }
            }

            System.out.print("Press any key to continue...");
            scan.nextLine();
            Liner.endLine();
        }
    }
}
