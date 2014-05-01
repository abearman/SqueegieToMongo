package db;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoURI;

import java.net.UnknownHostException;

/**
 * Created by AmyBear on 4/30/14.
 */
public class DBConnection {

    //private Statement stmt;
    //private Connection con;

    private Mongo mongo;
    private DBCollection coll;

    /**
     * Constructor connects to the database.
     */
    public DBConnection(String collection) {
        try {
            MongoURI mongoURI = new MongoURI(MyDBInfo.CONNECTION_STRING);
            mongo = new Mongo(mongoURI);
            DB db = mongo.getDB("venture");
            coll = db.getCollection(collection);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns a statement so that product catalog can do querying.
     */
    public DBCollection getCollection(){
        return coll;
    }

    /**
     * Closes the connection.
     */
    public void closeConnection() {
        mongo.close();
    }

}
