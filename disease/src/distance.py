from math import radians, sin, cos, sqrt, atan2

def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0  # Earth's radius in km

    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)

    a = sin(dlat / 2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c
    return distance

# Example usage:
lat1, lon1 = 13.7563, 100.5018   # Bangkok
lat2, lon2 = 18.7883, 98.9853    # Chiang Mai

print(f"Distance: {haversine(lat1, lon1, lat2, lon2):.2f} km")