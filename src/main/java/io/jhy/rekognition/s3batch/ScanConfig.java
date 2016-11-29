package io.jhy.rekognition.s3batch;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class ScanConfig {
    private static final String defaultFilter = "\\.(jpg|jpeg|png)$";
    private final Options options;
    private CommandLine args;
    private boolean threw;

    public ScanConfig(String[] inputArgs) {
        Options o = new Options();
        o.addOption(Option.builder("bucket").desc("S3 Bucket Name").hasArg().required().build());
        o.addOption(Option.builder("prefix").desc("S3 Bucket Prefix").hasArg().build());
        o.addOption(
            Option.builder("filter").desc("Key Filter Regex. Default '" + defaultFilter + "'").hasArg().build());
        o.addOption(
            Option.builder("profile").desc("AWS Credential Profile Name (in ~/.aws/credentials). Default 'default'")
                .hasArg().build());
        o.addOption(Option.builder("queue").desc("SQS Queue to populate. Will create if it doesn't exit.")
            .hasArg().required().build());
        o.addOption(Option.builder("max").desc("Max number of images to add to queue.").hasArg().build());
        o.addOption(Option.builder("help").desc("Get this help.").build());
        options = o;

        try {
            CommandLineParser parser = new DefaultParser();
            args = parser.parse(o, inputArgs);
        } catch (ParseException e) {
            threw = true;
        }
    }

    String bucket() {
        return args.getOptionValue("bucket");
    }

    String prefix() {
        return args.getOptionValue("prefix", "");
    }

    String filter() {
        return args.getOptionValue("filter", defaultFilter);
    }

    String profile() {
        return args.getOptionValue("profile", "default");
    }

    String queue() {
        return args.getOptionValue("queue");
    }

    String max() {
        return args.getOptionValue("max", "-1");
    }

    boolean needHelp() {
        return threw || args.hasOption("help");
    }

    void printHelp() {
        HelpFormatter helper = new HelpFormatter();
        helper.printHelp("scanner", options, true);
    }
}
