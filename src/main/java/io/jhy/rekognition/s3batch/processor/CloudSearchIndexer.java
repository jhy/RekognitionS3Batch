package io.jhy.rekognition.s3batch.processor;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.cloudsearchdomain.AmazonCloudSearchDomainClient;
import com.amazonaws.services.cloudsearchdomain.model.UploadDocumentsRequest;
import com.amazonaws.services.cloudsearchdomain.model.UploadDocumentsResult;
import com.amazonaws.services.cloudsearchv2.AmazonCloudSearchClient;
import com.amazonaws.services.cloudsearchv2.model.DomainStatus;
import com.amazonaws.services.rekognition.model.Label;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.gson.Gson;
import io.jhy.rekognition.s3batch.Logger;
import org.apache.commons.codec.Charsets;

import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class CloudSearchIndexer implements LabelProcessor {
    private AmazonCloudSearchDomainClient searchClient;

    public CloudSearchIndexer(AWSCredentialsProvider creds, String index) {
        // Find the Cloud Search Domain endpoint
        AmazonCloudSearchClient cloudsearch = new AmazonCloudSearchClient(creds);
        for (DomainStatus domain : cloudsearch.describeDomains().getDomainStatusList()) {
            Logger.Info(domain.getDomainName());
            if (domain.getDomainName().equals(index))
                searchClient = new AmazonCloudSearchDomainClient(creds)
                    .withEndpoint(domain.getDocService().getEndpoint());
        }
        if (searchClient == null) {
            Logger.Info("Could not find Cloud Search index %s, aborting.", index);
            throw new IllegalArgumentException("Unrecognized index.");
        }
    }

    @Override
    public void process(List<Label> labels, String path) {
        LabelInsertDoc doc = new LabelInsertDoc(labels, path);
        Logger.Debug("Json to push: \n%s", doc.asJson());

        byte[] jsonBytes = doc.asJsonBytes();
        UploadDocumentsRequest pushDoc = getUploadReq(jsonBytes);
        UploadDocumentsResult upRes = searchClient.uploadDocuments(pushDoc);

        Logger.Debug("Indexed %s, %s", path, upRes.getStatus());
    }

    private static UploadDocumentsRequest getUploadReq(byte[] doc) {
        return new UploadDocumentsRequest()
            .withDocuments(new ByteArrayInputStream(doc))
            .withContentLength((long) doc.length) // CS returns a HTML error if not set (and breaks the sdk json parser)
            .withContentType("application/json");
    }

    // wraps the label model into a doc that Cloud Search can insert
    private static class LabelInsertDoc {
        private static HashFunction idHash = Hashing.murmur3_128();
        private static Charset charset = Charsets.UTF_8;
        private static Gson gson = new Gson();

        // names CS is looking for:
        private String type = "add";
        private String id; // document id, hashed from path
        private LabelContent fields;

        public LabelInsertDoc(List<Label> labels, String imgPath) {
            id = idHash.hashString(imgPath, charset).toString();
            fields = new LabelContent(labels, imgPath);
        }

        String asJson() {
            // wrap it up in a list because CS expects a list of inserts, but we're doing 1:1 for queue mgmt and retries
            List<LabelInsertDoc> wrap = new ArrayList<>(1);
            wrap.add(this);
            return gson.toJson(wrap);
        }

        byte[] asJsonBytes() {
            return asJson().getBytes(charset);
        }
    }

    // model to insert to Cloud Search. GSON inspects the private fields.
    private static class LabelContent {
        private List<String> labels;
        private List<Integer> confidence;
        private String path;

        public LabelContent(List<Label> rekLabels, String imgPath) {
            path = imgPath;
            labels = new ArrayList<>(rekLabels.size());
            confidence = new ArrayList<>(rekLabels.size());
            for (Label label : rekLabels) {
                labels.add(label.getName());
                confidence.add(label.getConfidence().intValue()); // decimal precision not required
            }
        }
    }


}
