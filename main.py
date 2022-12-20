import mysql.connector
from mysql.connector import errorcode
import requests

try:
    cnx = mysql.connector.connect(user='root',
        password='firefighter77@',
        host='127.0.0.1',
        database='dataengineer')
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)

#connection cursor
cursor = cnx.cursor()

#add latitude column
add_lat_column = ("ALTER TABLE address ADD latitude DECIMAL(11, 8) DEFAULT NULL")
cursor.execute(add_lat_column)

#add longitude column
add_lon_column = ("ALTER TABLE address ADD longitude DECIMAL(11, 8) DEFAULT NULL")
cursor.execute(add_lon_column)



class Request:
    """Request OpenStreetMap's API"""
    def __init__(self):
        """Parameters for the API request"""
        self.url = 'https://nominatim.openstreetmap.org/search?'
        self.geo_coordinates = []
  
    def request_lat_lon(self, adress_id, db_address, db_city, db_postal_code):
        """Get the latitute and longitude of the address given in parametre.
        Store the result in a list with the adress id in the db, the latitude and longitude"""
        params = {
            'street': db_address,
            'city':db_city,
            'postalcode': db_postal_code,
            'format':'json',
            'limit': 1, #to avoid multiple results
        }
        try:
            request = requests.get(self.url, params)
            #print(request)
            request_result = request.json()
            #print(request_result)

            for val in request_result:
                try:
                    self.geo_coordinates.append([adress_id, val["lat"], val["lon"]])
                except:
                    print("Erreur dans la réception des données : ", val)

            #print("the geo coordinates", self.geo_coordinates)

        except:
            pass
        return self.geo_coordinates

    def add_lat_long(self):
        """Add latitude and longitude data into the address table using the address id"""
        for elt in self.geo_coordinates:
            add_values = ("UPDATE address SET latitude=%s, longitude=%s WHERE address_id=%s")
            data_geo = (elt[1], elt[2], elt[0])
            cursor.execute(add_values, data_geo)



new_address = Request()

#create the SQL query to prepared the API's request
query2 = ("SELECT address_id, address, city, postal_code FROM address")
cursor.execute(query2)

for (address_id, address, city, postal_code) in cursor:
    if postal_code != None:
        new_address.request_lat_lon(address_id, address, city, postal_code)

#send latitude and longitude to the address table
new_address.add_lat_long()

#Check briefly if the table is well filled
query3 = ("SELECT address_id, address, latitude, longitude FROM address limit 50")
cursor.execute(query3)
for (address_id, address, latitude, longitude) in cursor:
    print("voici les coordonnées geo: {}, {}, {}, {}".format(address_id, address, latitude, longitude))



# Make sure data is committed to the database
cnx.commit()

#close the cursor and connection
cursor.close()
cnx.close()
