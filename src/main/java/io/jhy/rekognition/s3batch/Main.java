package io.jhy.rekognition.s3batch;

import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        if (args.length > 0) {
            String first = args[0];
            if ("-scan".equals(first)) {
                Scanner.main(Arrays.copyOfRange(args, 1, args.length));
            } else if ("-process".equals(first)) {
                Processor.main(Arrays.copyOfRange(args, 1, args.length));
            } else {
                printHelp();
            }
        } else {
            printHelp();
        }
    }

    private static void printHelp() {
        System.out.println("Must run with -scan or -process as first arg");
    }
}
