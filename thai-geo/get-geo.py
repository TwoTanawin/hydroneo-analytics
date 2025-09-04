import os
import geopandas as gpd
from shapely.geometry import Point


class ThaiAdminLocator:
    def __init__(self, adm3_path: str):
        if not os.path.exists(adm3_path):
            raise FileNotFoundError(f"ADM3-like shapefile not found at: {adm3_path}")

        self.gdf = gpd.read_file(adm3_path)

        # Ensure CRS is WGS84 (EPSG:4326)
        if self.gdf.crs is None or self.gdf.crs.to_epsg() != 4326:
            self.gdf = self.gdf.to_crs(epsg=4326)

        # Store available column names
        cols = set(self.gdf.columns)

        # Column resolution
        self.ADM1_TH = self._pick_col(cols, ["ADM1_TH", "PROV_NAM_T", "prov_name_th", "prov_th", "province_th"])
        self.ADM1_EN = self._pick_col(cols, ["ADM1_EN", "PROV_NAM_E", "prov_name_en", "prov_en", "province_en"])
        self.ADM2_TH = self._pick_col(cols, ["ADM2_TH", "AMP_NAM_T", "amphoe_th", "dist_name_th", "district_th"])
        self.ADM2_EN = self._pick_col(cols, ["ADM2_EN", "AMP_NAM_E", "amphoe_en", "dist_name_en", "district_en"])
        self.ADM3_TH = self._pick_col(cols, ["ADM3_TH", "TAM_NAM_T", "tambon_th", "subdist_th", "subdistrict_th"])
        self.ADM3_EN = self._pick_col(cols, ["ADM3_EN", "TAM_NAM_E", "tambon_en", "subdist_en", "subdistrict_en"])

        self.ADM1_PC = self._pick_col(cols, ["ADM1_PCODE", "prov_code", "prov_id", "P_CODE_1"])
        self.ADM2_PC = self._pick_col(cols, ["ADM2_PCODE", "amp_code", "amphoe_id", "P_CODE_2"])
        self.ADM3_PC = self._pick_col(cols, ["ADM3_PCODE", "tam_code", "tambon_id", "P_CODE_3"])

        # Build spatial index
        _ = self.gdf.sindex

    def _pick_col(self, cols, candidates):
        for c in candidates:
            if c in cols:
                return c
        return None

    def latlon_to_admin(self, lat: float, lon: float, buffer_meters: float = 0.0):
        pt = Point(lon, lat)  # (x=lon, y=lat)

        # Fast candidate filter via spatial index
        bbox = (lon, lat, lon, lat)
        idx = list(self.gdf.sindex.intersection(bbox))
        if not idx:
            return None

        cand = self.gdf.iloc[idx]

        # Precise test
        match = cand[cand.contains(pt)]

        if match.empty and buffer_meters > 0:
            deg = buffer_meters / 111_000.0
            match = cand[cand.intersects(pt.buffer(deg))]

        if match.empty:
            return None

        row = match.iloc[0]
        return {
            "province_th": row.get(self.ADM1_TH),
            "province_en": row.get(self.ADM1_EN),
            "district_th": row.get(self.ADM2_TH),
            "district_en": row.get(self.ADM2_EN),
            "subdistrict_th": row.get(self.ADM3_TH),
            "subdistrict_en": row.get(self.ADM3_EN),
            "province_code": row.get(self.ADM1_PC),
            "district_code": row.get(self.ADM2_PC),
            "subdistrict_code": row.get(self.ADM3_PC),
        }
        
if __name__ == "__main__":
    adm3_path = "/Volumes/PortableSSD/Hydroneo/gis-Thai/thailand_gis/tambon/thailand_province_amphoe_tambon_simplify/thailand_province_amphoe_tambon_simplify.shp"
    locator = ThaiAdminLocator(adm3_path)

    # Example: Lat/Lon in Bangkok
    result = locator.latlon_to_admin(13.7563, 100.5018, buffer_meters=10)
    print(result)
