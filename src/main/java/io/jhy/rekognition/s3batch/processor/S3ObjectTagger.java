package io.jhy.rekognition.s3batch.processor;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.rekognition.model.Label;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.Tag;
import io.jhy.rekognition.s3batch.Processor;

import java.util.ArrayList;
import java.util.List;

/**
 * Writes labels back to the S3 Object that they were detected in. Uses the format {prefix}{label}={confidence}.
 * <p>
 *     S3 supports a max of 10 tags per object. This processor will override any existing tags that start with {prefix},
 *     but will leave other tags untouched. It will cap the total amount of tags to 10 (e.g. if an object already has
 *     8 other (non Rekognition label tags), the most it would add is 8 others.
 * </p>
 */
public class S3ObjectTagger implements LabelProcessor {
    private static final int maxTags = 10;

    private final AmazonS3Client s3;
    private final String tagPrefix;

    public S3ObjectTagger(AWSCredentialsProvider creds, String tagPrefix) {
        s3 = new AmazonS3Client(creds);
        this.tagPrefix = tagPrefix;
    }

    @Override
    public void process(List<Label> labels, String path) {
        Processor.PathSplit components = new Processor.PathSplit(path);
        String bucket = components.bucket;
        String key = components.key;

        // fetch the current set
        GetObjectTaggingResult tagging = s3.getObjectTagging(new GetObjectTaggingRequest(bucket, key));
        List<Tag> origTags = tagging.getTagSet();
        List<Tag> updateTags = new ArrayList<>();

        // copy the existing tags, but drop the ones matched by prefix (∴ leaves non-Rekognition label tags alone)
        for (Tag tag : origTags) {
            if (!tag.getKey().startsWith(tagPrefix))
                updateTags.add(tag);
        }
        
        // add the new ones
        for (Label label : labels) {
            if (updateTags.size() < maxTags)
                updateTags.add(new Tag(tagPrefix + label.getName(), label.getConfidence().toString()));
            else
                break;
        }

        // save it back
        s3.setObjectTagging(new SetObjectTaggingRequest(bucket, key, new ObjectTagging(updateTags)));
    }
}
