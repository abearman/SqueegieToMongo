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
public class YellowPagesToMongo {

    /* Instance variables */
    private static DBConnection db;
    private static DBCollection coll;

    public static void main(String args[]) {
        db = new DBConnection("activities");
        coll = db.getCollection();

        try {
            JSONArray array = new JSONArray(IOUtils.toString(new FileInputStream("src/main/java/java/squeegie_output/yellowpages-output.json")));
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
            if (object.has("address")) document.append("address", object.getString("address"));
            if (object.has("phone")) document.append("phone", object.getString("telephone"));
            if (object.has("website")) document.append("website", object.getString("website"));
            if (object.has("lat")) document.append("latitude", object.getDouble("lat"));
            if (object.has("lng")) document.append("longitude", object.getDouble("lng"));

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return document;
    }

    public static BasicDBObject parseMetadata(JSONObject object) throws JSONException {
        BasicDBObject metadataValue = new BasicDBObject();
        metadataValue.append("source", "yellowpages.com");
        if (object.has("yp_rating")) metadataValue.append("rating", object.getDouble("yp_rating"));
        if (object.has("yp_categories")) {
            if (object.get("yp_categories") instanceof JSONArray) {
                metadataValue.append("categories", com.mongodb.util.JSON.parse(object.get("yp_categories").toString()));
            } else {
                metadataValue.append("categories", object.get("yp_categories"));
            }
        }
        if (object.has("yp_review_count")) metadataValue.append("review_count", object.getInt("yp_review_count"));
        if (object.has("hours")) {
            if (object.get("hours") instanceof JSONArray) {
                metadataValue.append("hours", JSON.parse(object.get("hours").toString()));
            } else {
                metadataValue.append("hours", object.getString("hours"));
            }
        }
        if (object.has("yp_url")) metadataValue.append("url", object.getString("yp_url"));
        return metadataValue;
    }
}
