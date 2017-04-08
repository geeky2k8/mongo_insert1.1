package demo.mongo.client;

import java.net.UnknownHostException;

import demo.utils.MongoUtils;
/**
 * This class is intended for mongodb connection with java
 * 
 * @author Kishore Kumar
 * @version 1.0
 */
public class MongoClient {
    private static com.mongodb.MongoClient client = null;

    protected MongoClient() {
        // Exists only to defeat instantiation.
    }

    public static com.mongodb.MongoClient getInstance() {
        if (client == null) {
            try {
                client = new com.mongodb.MongoClient(MongoUtils.HOST, MongoUtils.PORT);
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }
        return client;
    }
}