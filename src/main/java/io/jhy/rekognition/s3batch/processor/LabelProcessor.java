package io.jhy.rekognition.s3batch.processor;

import com.amazonaws.services.rekognition.model.Label;

import java.util.List;

public interface LabelProcessor {
    public void process(List<Label>labels, String path);
}
