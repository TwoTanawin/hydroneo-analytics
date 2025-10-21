from geopy.geocoders import Nominatim

geolocator = Nominatim(user_agent="my-app-name")

latitude = 34.052235
longitude = -118.243683

location = geolocator.reverse((latitude, longitude), exactly_one=True)

if location:
    address = location.raw.get('address', {})
    province = address.get('state') or address.get('province')
    print("Province/State:", province)
