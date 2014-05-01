package db;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.util.JSON;
import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Created by AmyBear on 4/30/14.
 */
public class OpenTableToMongo {

    /* Instance variables */
    private static DBConnection db;
    private static DBCollection coll;

    public static void main(String args[]) {
        db = new DBConnection("activities");
        coll = db.getCollection();

        try {
            JSONArray array = new JSONArray(IOUtils.toString(new FileInputStream("src/main/java/java/squeegie_output/yelp-output.json")));
            for (int i = 0; i < array.length(); i++) {
                JSONObject object = array.getJSONObject(i);
                if (object.has("title")) {
                    BasicDBObject query = getQuery(object);
                    BasicDBObject document = parseJSON(object);
                    BasicDBObject metadata = parseMetadata(object);
                    if (query != null && document != null) {
                        coll.update(query, document, true, false);
                        coll.update(document, new BasicDBObject("$addToSet", new BasicDBObject("metadata", metadata)));
                        System.out.println("One updated: " + object.getString("title"));
                    } else {
                        System.out.println("Something went wrong with either the query or the document, or both");
                    }
                }
            }

        } catch (JSONException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("All done!");
    }

    public static BasicDBObject getQuery(JSONObject object) {
        BasicDBObject query = new BasicDBObject();
        try {
            if (object.has("title")) query = new BasicDBObject("title", object.getString("title"));
            if (object.has("address")) query.append("address", object.getString("address"));
            return query;
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return query;
    }

    public static BasicDBObject parseJSON(JSONObject object) {
        BasicDBObject document = null;
        try {
            document = new BasicDBObject(); // Document to insert into Mongo
            if (object.has("title")) document.append("title", object.getString("title"));
            if (object.has("phone")) document.append("phone", object.getString("phone"));
            if (object.has("address")) document.append("address", object.getString("address"));

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return document;
    }

    public static BasicDBObject parseMetadata(JSONObject object) throws JSONException {
        BasicDBObject metadataValue = new BasicDBObject();
        metadataValue.append("source", "yelp.com");
        if (object.has("yelp_rating")) metadataValue.append("rating", object.getDouble("yelp_rating"));
        if (object.has("yelp_categories")) {
            if (object.get("yelp_categories") instanceof JSONArray) {
                metadataValue.append("categories", JSON.parse(object.get("yelp_categories").toString()));
            } else {
                metadataValue.append("categories", object.get("yelp_categories"));
            }
        }
        if (object.has("yelp_review_count")) metadataValue.append("review_count", object.getInt("yelp_review_count"));
        if (object.has("yelp_url")) metadataValue.append("url", object.getString("yelp_url"));
        return metadataValue;
    }

}
