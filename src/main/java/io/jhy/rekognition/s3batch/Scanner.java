package io.jhy.rekognition.s3batch;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

public class Scanner {
    private AmazonS3Client s3;
    private AmazonSQSClient sqs;
    private String queueUrl;
    private ScanConfig opt;
    private AtomicLong numSeen = new AtomicLong(0);
    private long max;
    private Pattern filter;

    public static void main(String[] args) {
        ScanConfig opt = new ScanConfig(args);
        if (opt.needHelp()) {
            opt.printHelp();
            return;
        }

        Scanner app = new Scanner(opt);
        app.scanBucket();
    }

    public Scanner(ScanConfig config) {
        opt = config;
        ProfileCredentialsProvider creds = new ProfileCredentialsProvider(opt.profile());
        creds.getCredentials(); // credible credential criteria
        s3 = new AmazonS3Client(creds);
        sqs = new AmazonSQSClient(creds);

        CreateQueueResult queueResult = sqs.createQueue(opt.queue());
        queueUrl = queueResult.getQueueUrl();

        filter = Pattern.compile(opt.filter(), Pattern.CASE_INSENSITIVE);
        max = Long.parseLong(opt.max());
    }

    public void scanBucket() {
        ListObjectsRequest listReq = new ListObjectsRequest()
            .withPrefix(opt.prefix())
            .withBucketName(opt.bucket());

        Logger.Info("Scanning S3 bucket %s %s", opt.bucket(), opt.prefix());
        ObjectListing listing = s3.listObjects(listReq);
        boolean ok = processObjects(listing.getObjectSummaries());

        while (ok && listing.isTruncated()) {
            listing = s3.listNextBatchOfObjects(listing);
            ok = processObjects(listing.getObjectSummaries());
        }

        Logger.Info("Completed scan, added %s images to the processing queue.", numSeen.get());
    }

    private boolean processObjects(List<S3ObjectSummary> objects) {
        Logger.Debug("Scanning next batch of %s ", objects.size());
        objects
            .parallelStream()
            .filter(this::shouldEnqueue)
            .forEach(object -> {
                numSeen.incrementAndGet();
                String path = object.getBucketName() + "/" + object.getKey();
                Logger.Info("Posting: %s", path);

                SendMessageRequest msg = new SendMessageRequest()
                    .withQueueUrl(queueUrl)
                    .withMessageBody(path);
                sqs.sendMessage(msg);
            });
        if (max > -1L && numSeen.incrementAndGet() > max) {
            Logger.Info("Added max jobs, quitting");
            return false;
        }
        return true;
    }

    // todo - interface method passed in scanBucket if you need more than a regex
    private boolean shouldEnqueue(S3ObjectSummary object) {
        return filter.matcher(object.getKey()).find();
    }
}
