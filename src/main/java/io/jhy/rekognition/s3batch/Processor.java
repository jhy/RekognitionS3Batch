package io.jhy.rekognition.s3batch;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.rekognition.AmazonRekognitionClient;
import com.amazonaws.services.rekognition.model.DetectLabelsRequest;
import com.amazonaws.services.rekognition.model.DetectLabelsResult;
import com.amazonaws.services.rekognition.model.Image;
import com.amazonaws.services.rekognition.model.InvalidParameterException;
import com.amazonaws.services.rekognition.model.Label;
import com.amazonaws.services.rekognition.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import io.jhy.rekognition.s3batch.processor.CloudSearchIndexer;
import io.jhy.rekognition.s3batch.processor.DynamoWriter;
import io.jhy.rekognition.s3batch.processor.LabelProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class Processor {
    private AmazonSQSClient sqs;
    private String queueUrl;
    private AtomicLong numSeen = new AtomicLong(0);
    private long maxImagesToProcess;
    private ThreadPoolExecutor executor;
    private AmazonRekognitionClient rek;
    private List<LabelProcessor> processors = new ArrayList<>();
    private float minConfidence;

    public static void main(String[] args) {
        ProcessorConfig opt = new ProcessorConfig(args);
        if (opt.needHelp()) {
            opt.printHelp();
            return;
        }

        Processor processor = new Processor(opt);
        processor.start();
    }

    public Processor(ProcessorConfig config) {
        ProfileCredentialsProvider creds = new ProfileCredentialsProvider(config.profile());
        creds.getCredentials(); // credible credential criteria

        if (config.disableCerts())
            System.setProperty("com.amazonaws.sdk.disableCertChecking", "true");

        // Rekognition init
        rek = new AmazonRekognitionClient(creds);
        if (config.endpointOverride())
            rek.setEndpoint(config.endpoint());
        minConfidence = Integer.parseInt(config.confidence());


        // The SQS queue to find jobs on
        sqs = new AmazonSQSClient(creds);
        queueUrl = sqs.createQueue(config.queue()).getQueueUrl();

        // Processors
        if (config.wantCloudSearch())
            processors.add(new CloudSearchIndexer(creds, config.cloudSearch()));
        if (config.wantDynamo())
            processors.add(new DynamoWriter(creds, config.dynamo()));

        // Executor Service
        int maxWorkers = Integer.parseInt(config.concurrency());
        executor = new ThreadPoolExecutor(
            1, maxWorkers, 30, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(maxWorkers * 2, false),
            new CallerRunsPolicy() // prevents backing up too many jobs
        );

        maxImagesToProcess = Long.parseLong(config.max());
    }

    public void start() {
        if (processors.isEmpty()) {
            Logger.Info("No processors defined, will not start up.");
            return;
        }

        Logger.Info("Processor started up, looking for messages on %s", queueUrl);

        while (!executor.isShutdown()) {
            // poll for messages on the queue.
            ReceiveMessageRequest poll = new ReceiveMessageRequest(queueUrl)
                .withMaxNumberOfMessages(10)
                .withWaitTimeSeconds(20);
            List<Message> messages = sqs.receiveMessage(poll).getMessages();
            Logger.Debug("Got %s messages from queue. Processed %s so far.", messages.size(), numSeen.get());

            // process the messages in parallel.
            for (Message message : messages) {
                numSeen.incrementAndGet();
                executor.execute(() -> {
                    try {
                        processTask(message);
                    } catch (InvalidParameterException e) {
                        if (e.getMessage().contains("Minimum image height")) {
                            Logger.Debug("Input image %s too small to analyze, skipping.", message.getBody());
                        }
                    }
                });

                // remove the job from the queue when completed successfully (or skipped)
                sqs.deleteMessage(queueUrl, message.getReceiptHandle());
            }
            if (maxImagesToProcess > -1 && numSeen.get() > maxImagesToProcess) {
                Logger.Info("Seen enough (%s), quitting.", numSeen.get());
                executor.shutdownNow();
            }

            // error handling is simple here - an exception will terminate just the impacted job, and the job is left
            // on the queue, so you can fix and re-drive. Alternatively you could catch and write to a dead letter queue
        }
    }

    private void processTask(Message message) {
        String path = message.getBody();
        String bucket = path.substring(0, path.indexOf('/'));
        String key = path.substring(bucket.length() + 1);
        Logger.Info("Processing %s %s", bucket, key);

        // Rekognition: Detect Labels from S3 object
        DetectLabelsRequest req = new DetectLabelsRequest()
            .withImage(new Image().withS3Object(new S3Object().withBucket(bucket).withName(key)))
            .withMinConfidence(minConfidence);
        DetectLabelsResult result;
        result = rek.detectLabels(req);
        List<Label> labels = result.getLabels();
        Logger.Debug("In %s, found: %s", key, labels);

        // Process downstream actions:
        for (LabelProcessor processor : processors) {
            processor.process(labels, path);
        }
    }

    public Processor addLabelProcessor(LabelProcessor processor) {
        processors.add(processor);
        return this;
    }
}
