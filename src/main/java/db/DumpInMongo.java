package db;

import specific_squeegie_converters.SqueegieToMongo;

/**
 * Created by AmyBear on 5/8/14.
 */
public class DumpInMongo {

    public static void main(String args[]) {
        /*System.out.println("Starting restaurant Squeegie files");
        for (String filename : Sources.restaurantJSONFiles) {
            System.out.println("Starting squeeging with file: " + filename);
            SqueegieToMongo stm = new SqueegieToMongo(filename);
            stm.doSqueeging();
            System.out.println("Done with squeeging with file: " + filename);
        }
        System.out.println("Done with restaurant Squeegie files");*/

        System.out.println("Starting other Squeegie files");
        for (String filename : Sources.otherJSONFiles) {
            System.out.println("Starting squeeging with file: " + filename);
            SqueegieToMongo stm = new SqueegieToMongo(filename);
            stm.doSqueeging();
            System.out.println("Done with squeeging with file: " + filename);
        }
        System.out.println("Done with other Squeegie files");
    }

}
