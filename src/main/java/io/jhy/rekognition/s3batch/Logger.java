package io.jhy.rekognition.s3batch;

public class Logger {
    public static void Info(String msg, Object... args) {
        System.out.println(String.format(msg, args));
    }

    public static void Debug(String msg, Object... args) {
        System.out.println(String.format(msg, args));
    }
}
