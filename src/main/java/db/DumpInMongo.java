package db;

import specific_squeegie_converters.SqueegieToMongo;

/**
 * Created by AmyBear on 5/8/14.
 */
public class DumpInMongo {

    public static void main(String args[]) {
        for (String filename : Sources.jsonFiles) {
            System.out.println("Starting squeeging with file: " + filename);
            SqueegieToMongo stm = new SqueegieToMongo(filename);
            stm.doSqueeging();
            System.out.println("Done with squeeging with file: " + filename);
        }
    }

}
