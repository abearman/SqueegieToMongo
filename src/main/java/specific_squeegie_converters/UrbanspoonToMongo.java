package specific_squeegie_converters;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.util.JSON;
import db.DBConnection;
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
public class UrbanspoonToMongo {

    /* Instance variables */
    private static DBConnection db;
    private static DBCollection coll;

    public static void main(String args[]) {
        db = new DBConnection("activities");
        coll = db.getCollection();

        try {
            JSONArray array = new JSONArray(IOUtils.toString(new FileInputStream("src/main/java/java/squeegie_output/urbanspoon-output.json")));
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
            if (object.has("lat")) query.append("lat", object.getDouble("lat"));
            if (object.has("lng")) query.append("lng", object.getDouble("lng"));
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
            if (object.has("lat")) document.append("latitude", object.getDouble("lat"));
            if (object.has("lng")) document.append("longitude", object.getDouble("lng"));
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return document;
    }

    public static BasicDBObject parseMetadata(JSONObject object) throws JSONException {
        BasicDBObject metadataValue = new BasicDBObject();
        metadataValue.append("source", "urbanspoon.com");
        if (object.has("urbanspoon_rating")) metadataValue.append("rating", object.getDouble("urbanspoon_rating"));
        if (object.has("urbanspoon_categories")) {
            if (object.get("urbanspoon_categories") instanceof JSONArray) {
                metadataValue.append("categories", com.mongodb.util.JSON.parse(object.get("urbanspoon_categories").toString()));
            } else {
                metadataValue.append("categories", object.get("urbanspoon_categories"));
            }
        }
        if (object.has("urbanspoon_votes")) metadataValue.append("review_count", object.getInt("urbanspoon_votes"));
        if (object.has("urbanspoon_reviews"))
            metadataValue.append("professional_review_count", object.getInt("urbanspoon_reviews"));
        if (object.has("urbanspoon_url")) metadataValue.append("url", object.getString("urbanspoon_url"));
        if (object.has("type")) metadataValue.append("type", object.getString("type"));
        if (object.has("urbanspoon-attributes")) {
            if (object.get("urbanspoon_attributes") instanceof JSONArray) {
                metadataValue.append("attributes", JSON.parse(object.get("urbanspoon_attributes").toString()));
            } else {
                metadataValue.append("attributes", object.getString("urbanspoon_attributes"));
            }
        }
        if (object.has("urbanspoon_images")) {
            if (object.get("urbanspoon_images") instanceof JSONArray) {
                metadataValue.append("images", JSON.parse(object.get("urbanspoon_images").toString()));
            } else {
                metadataValue.append("images", object.getString("urbanspoon_images"));
            }
        }
        return metadataValue;
    }
}
