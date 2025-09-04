package net.hydroneo;

import java.util.Map;

public class Main {
    public static void main(String[] args) {
        String path = "/Volumes/PortableSSD/Hydroneo/gis-Thai/thailand_gis/tambon/thailand_province_amphoe_tambon_simplify/thailand_province_amphoe_tambon_simplify.shp";
        
        try {
            ThaiAdminLocator locator = new ThaiAdminLocator(path);
            Map<String, Object> result = locator.latlonToAdmin(13.7563, 100.5018, 10);

            if (result != null) {
                result.forEach((k, v) -> System.out.println(k + ": " + v));
            } else {
                System.out.println("No match found.");
            }

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
