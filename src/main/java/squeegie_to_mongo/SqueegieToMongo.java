package squeegie_to_mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import com.mongodb.util.JSON;
import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by AmyBear on 5/8/14.
 */
public class SqueegieToMongo {

    /* Instance variables */
    private static DBConnection _db;
    private static DBCollection _coll;
    private static String _filename;

    public SqueegieToMongo(String filename) {
        _db = new DBConnection("activities");
        _coll = _db.getCollection();
        _filename = filename;
    }

    public static void doSqueeging() {
        try {
            // Gets the JSONArray of Squeegie objects
            JSONArray array = new JSONArray(IOUtils.toString(new FileInputStream("src/main/java/squeegie_output/" + _filename)));

            // Iterates through each object in the JSONArray
            for (int i = 0; i < array.length(); i++) {
                JSONObject object = array.getJSONObject(i);

                // Only proceeds if the object has a title (ignores null entries)
                if (object.has("title")) {
                    BasicDBObject query = getQuery(object);
                    BasicDBObject document = parseJSON(object);
                    BasicDBObject metadata = parseMetadata(object);

                    if (query != null && document != null) {
                        if (_coll.findOne(query) != null) { // There is already an entry that matches this query
                            updateExistingDocument(query, document, metadata);
                        } else { // Insert a brand, spanking new document
                            insertNewDocument(query, document, metadata);
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
    }

    public static void updateExistingDocument(BasicDBObject query, BasicDBObject document, BasicDBObject metadata) {
        // Retrieves the existing document from the database
        DBObject found = _coll.findOne(query);

        // Updates the existing document by adding the new set of metadata
        _coll.update(query, new BasicDBObject("$addToSet", new BasicDBObject("metadata", metadata)));

        // Updates the existing document to fill in any missing basic info (which goes outside metadata)
        _coll.update(query, constructSetObject(found, document, metadata));
    }

    public static void insertNewDocument(BasicDBObject query, BasicDBObject document, BasicDBObject metadata) {
        document.append("type", getTypeOfActivity(metadata));
        _coll.insert(document);
        _coll.update(query, new BasicDBObject("$addToSet", new BasicDBObject("metadata", metadata)));
    }

    public static Object getTypeOfActivity(BasicDBObject metadata) {
        if (Arrays.asList(Sources.restaurantJSONFiles).contains(_filename)) {
            return "restaurant";
        } else if (Arrays.asList(Sources.otherJSONFiles).contains(_filename)) {
            Object type = metadata.get("type");
            if (type instanceof JSONArray) {
                return JSON.parse(type.toString());
            } else {
                return type;
            }
        }
        return "";
    }

    public static BasicDBObject constructSetObject(DBObject found, BasicDBObject document, BasicDBObject metadata) {
        BasicDBObject set = new BasicDBObject();
        if (!found.containsField("latitude") && document.containsField("latitude"))
            set.append("$set", new BasicDBObject("latitude", document.getString("latitude")));
        if (!found.containsField("longitude") && document.containsField("longitude"))
            set.append("$set", new BasicDBObject("longitude", document.getString("longitude")));
        if (!found.containsField("phone") && document.containsField("phone"))
            set.append("$set", new BasicDBObject("phone", document.getString("phone")));
        if (!found.containsField("address") && document.containsField("address"))
            set.append("$set", new BasicDBObject("address", document.getString("address")));
        if (!found.containsField("website") && document.containsField("website"))
            set.append("$set", new BasicDBObject("website", document.getString("website")));

        set.append("$set", new BasicDBObject("type", getTypeOfActivity(metadata)));
        return set;
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
        if (!metadataValue.containsField("source")) {
            String[] resource = _filename.split("\\-");
            metadataValue.append("source", resource[0]);
        }
        return metadataValue;
    }

}
