package io.jhy.rekognition.s3batch.processor;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.rekognition.model.Label;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.commons.codec.Charsets;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DynamoWriter implements LabelProcessor {
    private static HashFunction idHash = Hashing.murmur3_128();
    private static Charset charset = Charsets.UTF_8;

    private final Table table;

    public DynamoWriter(AWSCredentialsProvider creds, String dynamoTable) {
        AmazonDynamoDBClient client = new AmazonDynamoDBClient(creds);
        DynamoDB db = new DynamoDB(client);
        table = db.getTable(dynamoTable);
    }

    @Override
    public void process(List<Label> labels, String imgPath) {

        String id = idHash.hashString(imgPath, charset).toString();
        Item item = new Item()
            .withPrimaryKey("id", id)
            .withString("path", imgPath)
            .withList("labels", labels.stream().map(DynamoWriter::labelToFields).collect(Collectors.toList()));

        table.putItem(item);
    }

    private static Map<String, Object> labelToFields(Label label) {
        HashMap<String, Object> map = new HashMap<>();
        map.put("name", label.getName());
        map.put("confidence", label.getConfidence());
        return map;
    }

}
