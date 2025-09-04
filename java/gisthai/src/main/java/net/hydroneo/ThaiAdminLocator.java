package net.hydroneo;

import java.io.File;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.geotools.api.data.FileDataStore;
import org.geotools.api.data.FileDataStoreFinder;
import org.geotools.api.data.SimpleFeatureSource;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.geometry.jts.JTSFactoryFinder;

import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

public class ThaiAdminLocator {

    private final SimpleFeatureCollection featureCollection;

    private String ADM1_TH, ADM1_EN, ADM2_TH, ADM2_EN, ADM3_TH, ADM3_EN;
    private String ADM1_PC, ADM2_PC, ADM3_PC;

    public ThaiAdminLocator(String shapefilePath) throws Exception {
        File file = new File(shapefilePath);
        if (!file.exists()) {
            throw new RuntimeException("ADM3-like shapefile not found at: " + shapefilePath);
        }

        FileDataStore store = FileDataStoreFinder.getDataStore(file);
        SimpleFeatureSource featureSource = store.getFeatureSource();
        featureCollection = featureSource.getFeatures();

        // Auto-resolve columns
        resolveColumns(featureCollection.getSchema().getAttributeDescriptors().stream()
            .map(attr -> attr.getLocalName())
            .toList());
    }

    private void resolveColumns(List<String> cols) {
        ADM1_TH = pickCol(cols, Arrays.asList("ADM1_TH", "PROV_NAM_T", "prov_name_th", "prov_th", "province_th"));
        ADM1_EN = pickCol(cols, Arrays.asList("ADM1_EN", "PROV_NAM_E", "prov_name_en", "prov_en", "province_en"));
        ADM2_TH = pickCol(cols, Arrays.asList("ADM2_TH", "AMP_NAM_T", "amphoe_th", "dist_name_th", "district_th"));
        ADM2_EN = pickCol(cols, Arrays.asList("ADM2_EN", "AMP_NAM_E", "amphoe_en", "dist_name_en", "district_en"));
        ADM3_TH = pickCol(cols, Arrays.asList("ADM3_TH", "TAM_NAM_T", "tambon_th", "subdist_th", "subdistrict_th"));
        ADM3_EN = pickCol(cols, Arrays.asList("ADM3_EN", "TAM_NAM_E", "tambon_en", "subdist_en", "subdistrict_en"));

        ADM1_PC = pickCol(cols, Arrays.asList("ADM1_PCODE", "prov_code", "prov_id", "P_CODE_1"));
        ADM2_PC = pickCol(cols, Arrays.asList("ADM2_PCODE", "amp_code", "amphoe_id", "P_CODE_2"));
        ADM3_PC = pickCol(cols, Arrays.asList("ADM3_PCODE", "tam_code", "tambon_id", "P_CODE_3"));
    }

    private String pickCol(List<String> cols, List<String> candidates) {
        for (String c : candidates) {
            if (cols.contains(c)) return c;
        }
        return null;
    }

    public Map<String, Object> latlonToAdmin(double lat, double lon, double bufferMeters) {
        GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
        Point point = geometryFactory.createPoint(new Coordinate(lon, lat));

        try (SimpleFeatureIterator features = featureCollection.features()) {
            while (features.hasNext()) {
                SimpleFeature feature = features.next();
                Geometry geom = (Geometry) feature.getDefaultGeometry();

                boolean match = geom.contains(point);

                if (!match && bufferMeters > 0) {
                    double bufferDeg = bufferMeters / 111_000.0;
                    match = geom.intersects(point.buffer(bufferDeg));
                }

                if (match) {
                    Map<String, Object> result = new LinkedHashMap<>();
                    result.put("province_th", feature.getAttribute(ADM1_TH));
                    result.put("province_en", feature.getAttribute(ADM1_EN));
                    result.put("district_th", feature.getAttribute(ADM2_TH));
                    result.put("district_en", feature.getAttribute(ADM2_EN));
                    result.put("subdistrict_th", feature.getAttribute(ADM3_TH));
                    result.put("subdistrict_en", feature.getAttribute(ADM3_EN));
                    result.put("province_code", feature.getAttribute(ADM1_PC));
                    result.put("district_code", feature.getAttribute(ADM2_PC));
                    result.put("subdistrict_code", feature.getAttribute(ADM3_PC));
                    return result;
                }
            }
        }
        return null;
    }
}
