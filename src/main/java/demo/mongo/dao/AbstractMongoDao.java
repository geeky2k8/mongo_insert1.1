package demo.mongo.dao;

import java.util.List;

import com.mongodb.DB;
import com.mongodb.DBObject;

import demo.mongo.client.MongoClient;
import demo.utils.MongoUtils;
/**
 * This class is intended providing mongodb common method
 * 
 * @author Kishore Kumar
 * @version 1.0
 */
public class AbstractMongoDao {
    private static DB db = null;

    public AbstractMongoDao() {
        db = MongoClient.getInstance().getDB(getDBname());
    }

    private String getDBname() {
        return MongoUtils.DB_NAME;
    }

    public void insert(String collectionName, List<DBObject> dbObjects) {
        db.getCollection(collectionName).insert(dbObjects);
    }

}
