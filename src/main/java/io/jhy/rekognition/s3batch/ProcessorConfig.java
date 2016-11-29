package io.jhy.rekognition.s3batch;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class ProcessorConfig {
    private final Options options;
    private CommandLine args;
    private boolean threw;

    public ProcessorConfig(String[] inputArgs) {
        Options o = new Options();
        o.addOption(Option.builder("queue").desc("SQS Queue to fetch tasks from.")
            .hasArg().required().build());
        o.addOption(
            Option.builder("profile").desc("AWS Credential Profile Name (in ~/.aws/credentials). Default 'default'")
                .hasArg().build());
        o.addOption(Option.builder("help").desc("Get this help.").build());
        o.addOption(Option.builder("dynamo").desc("Dynamo DB table to optionally insert into.").hasArg().build());
        o.addOption(Option.builder("cloudsearch").desc("Cloud Search index to optionally insert into.").hasArg().build());
        o.addOption(Option.builder("max").desc("Max number of images to index.").hasArg().build());
        o.addOption(
            Option.builder("concurrency").desc("Number of concurrent Rekognition jobs. Default 20").hasArg().build());
        o.addOption(Option.builder("disablecerts").desc("Disable certificate checking.").build());
        o.addOption(Option.builder("endpoint").desc("Override the Rekognition endpoint.").hasArg().build());
        o.addOption(Option.builder("confidence").desc("Minimum confidence in labels. Default 70.").hasArg().build());

        options = o;

        try {
            CommandLineParser parser = new DefaultParser();
            args = parser.parse(o, inputArgs);
        } catch (ParseException e) {
            threw = true;
        }
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

    String cloudSearch() {
        return args.getOptionValue("cloudsearch");
    }

    boolean wantCloudSearch() {
        return args.hasOption("cloudsearch");
    }

    String dynamo() {
        return args.getOptionValue("dynamo");
    }

    boolean wantDynamo() {
        return args.hasOption("dynamo");
    }

    boolean disableCerts() {
        return args.hasOption("disablecerts");
    }

    boolean endpointOverride() {
        return args.hasOption("endpoint");
    }

    String endpoint() {
        return args.getOptionValue("endpoint");
    }

    String confidence() {
        return args.getOptionValue("confidence", "70");
    }

    String concurrency() {
        return args.getOptionValue("concurrency", "20");
    }

    boolean needHelp() {
        return threw || args.hasOption("help");
    }

    void printHelp() {
        HelpFormatter helper = new HelpFormatter();
        helper.printHelp("scanner", options, true);
    }
}
