package specific_squeegie_converters;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import com.mongodb.util.JSON;
import db.DBConnection;
import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

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
                        if (coll.findOne(query) != null) {
                            DBObject found = coll.findOne(query);
                            coll.update(query, new BasicDBObject("$addToSet", new BasicDBObject("metadata", metadata)));

                            BasicDBObject set = new BasicDBObject();

                            if (!found.containsField("latitude"))
                                set.append("$set", new BasicDBObject("latitude", document.getString("latitude")));
                            if (!found.containsField("longitude"))
                                set.append("$set", new BasicDBObject("longitude", document.getString("longitude")));
                            if (!found.containsField("phone"))
                                set.append("$set", new BasicDBObject("phone", document.getString("phone")));
                            if (!found.containsField("address"))
                                set.append("$set", new BasicDBObject("address", document.getString("address")));
                            if (!found.containsField("website"))
                                set.append("$set", new BasicDBObject("website", document.getString("website")));

                            coll.update(query, set);

                        } else {
                            coll.insert(document);
                            coll.update(query, new BasicDBObject("$addToSet", new BasicDBObject("metadata", metadata)));
                        }
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
        try {
            if (object.has("address") && object.has("lat") && object.has("lng")) {
                String address = object.getString("address");
                address = address.replaceAll("\\.", "");
                address = address.replaceAll("\\,", "");
                address = address.toLowerCase();

                DBObject query = QueryBuilder.start("title").is(object.getString("title")).or(
                        QueryBuilder.start("address").is(address).get(),
                        QueryBuilder.start().and("latitude").is(object.getDouble("lat")).and("longitude").is(object.getDouble("lng")).get()
                ).get();
                return (BasicDBObject) query;
            } else {
                BasicDBObject query = new BasicDBObject("title", object.getString("title"));
                if (object.has("address")) {
                    String address = object.getString("address");
                    address = address.replaceAll("\\.", "");
                    address = address.replaceAll("\\,", "");
                    address = address.toLowerCase();
                    query.append("address", address);
                } else if (object.has("lat") && object.has("lng")) {
                    query.append("latitude", object.getDouble("lat"));
                    query.append("longitude", object.getDouble("lng"));
                }
                return query;
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static BasicDBObject parseJSON(JSONObject object) {
        BasicDBObject document = null;
        try {
            document = new BasicDBObject(); // Document to insert into Mongo
            if (object.has("title")) document.append("title", object.getString("title"));
            if (object.has("phone")) document.append("phone", object.getString("phone"));
            if (object.has("address")) {
                String address = object.getString("address");
                address = address.replaceAll("\\.", "");
                address = address.replaceAll("\\,", "");
                address = address.toLowerCase();
                document.append("address", address);
            }
            if (object.has("website")) document.append("website", object.getString("website"));
            if (object.has("lat")) document.append("latitude", object.getDouble("lat"));
            if (object.has("lng")) document.append("longitude", object.getDouble("lng"));

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return document;
    }

    public static boolean keyIsNotOfMainType(String key) {
        if (key.equals("title") || key.equals("address") || key.equals("lat") || key.equals("lng") || key.equals("phone") || key.equals("website")) {
            return false;
        }
        return true;
    }

    public static BasicDBObject parseMetadata(JSONObject object) throws JSONException {
        BasicDBObject metadataValue = new BasicDBObject();
        Iterator<?> keys = object.keys();
        while (keys.hasNext()) {
            String key = (String) keys.next();
            Object obj = object.get(key);
            if (keyIsNotOfMainType(key)) {
                if (obj instanceof JSONArray) {
                    metadataValue.append(key, JSON.parse(obj.toString()));
                } else {
                    metadataValue.append(key, obj);
                }
            }
        }
        metadataValue.append("source", "yellowpages.com");
        return metadataValue;
    }
}
