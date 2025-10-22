from geopy.geocoders import Nominatim

geolocator = Nominatim(user_agent="my-app-name")

latitude = 13.7063
longitude = 100.4597

location = geolocator.reverse((latitude, longitude), exactly_one=True)

if location:
    address = location.raw.get('address', {})
    province = address.get('state') or address.get('province')
    print("Province/State:", province)
