import urllib2, base64
import datetime
from xml.etree import ElementTree as etree
from dateutil.parser import parse
import psycopg2
import sys

## Initializations

## Feed auth
global feed_username
feed_username = "USER"
global feed_password
feed_password = "PASSWORD"

## DB auth
global db
db = "DB"
global db_user
db_user = "DB_USER"

feeds = {
  "car_parks":"http://www.traffcom.org/datex2/carparks/content.xml",
  "detector_definition":"http://www.traffcom.org/datex2/detectordefinition/content.xml",
  "detector":"http://www.traffcom.org/datex2/detectorsettings/content.xml",
  "scoot_definition":"http://www.traffcom.org/datex2/scootdefinition/content.xml",
  "scoot_settings":"http://www.traffcom.org/datex2/scootsettings/content.xml",
  "events":"http://www.traffcom.org/datex2/trafficevents/content.xml"
}

#### Function definitions ####

def get_feed_xml(feed):
  
  ## Request feed 
  request = urllib2.Request(feed)
  base64string = base64.encodestring('%s:%s' % (feed_username, feed_password)).replace('\n', '')
  request.add_header("Authorization", "Basic %s" % base64string)   
  result = urllib2.urlopen(request)
  
  ## Parse XML
  xml = result.read()
  root = etree.fromstring(xml)
  
  return root
  
def get_measurements(root, feed):  
    
  ## Get measurement data
  if feed == "http://www.traffcom.org/datex2/scootsettings/content.xml":
    measurements = root[1].findall('{http://datex2.eu/schema/1_0/1_0}siteMeasurements')
    pub_time = parse(root[1].findall("./{http://datex2.eu/schema/1_0/1_0}publicationTime")[0].text)
  
  elif feed == "http://www.traffcom.org/datex2/scootdefinition/content.xml":
    measurements = root[1][3].findall('{http://datex2.eu/schema/1_0/1_0}measurementSiteRecord')
    pub_time = parse(root[1].findall("./{http://datex2.eu/schema/1_0/1_0}publicationTime")[0].text)
  #
  # elif feed == "http://www.traffcom.org/datex2/carparks/content.xml"
  #   measurements = root[1][3].findall('{http://datex2.eu/schema/1_0/1_0}measurementSiteRecord')
  #   pub_time = parse(root[1].findall("./{http://datex2.eu/schema/1_0/1_0}publicationTime")[0].text)
  
  return measurements, pub_time

## Get site location for measurements
def get_site_location(measurement, feed, locations):
  
  ## For congestion feed (definition -> settings)
  if feed == "http://www.traffcom.org/datex2/scootdefinition/content.xml":
      measurement_location = measurement.findall("{http://datex2.eu/schema/1_0/1_0}measurementSiteReference")[0].text
      
      ## Parse xml into dictionary
      for location in locations:
        if location.attrib['id'] == measurement_location:
          dict = {
            "from_description" : location.findall(".//{http://datex2.eu/schema/1_0/1_0}from/{http://datex2.eu/schema/1_0/1_0}name/{http://datex2.eu/schema/1_0/1_0}descriptor/{http://datex2.eu/schema/1_0/1_0}value")[0].text,
            "to_description" : location.findall(".//{http://datex2.eu/schema/1_0/1_0}to/{http://datex2.eu/schema/1_0/1_0}name/{http://datex2.eu/schema/1_0/1_0}descriptor/{http://datex2.eu/schema/1_0/1_0}value")[0].text,
            "from_lat" : location.findall(".//{http://datex2.eu/schema/1_0/1_0}from/{http://datex2.eu/schema/1_0/1_0}pointCoordinates/{http://datex2.eu/schema/1_0/1_0}latitude")[0].text,
            "from_lng" : location.findall(".//{http://datex2.eu/schema/1_0/1_0}from/{http://datex2.eu/schema/1_0/1_0}pointCoordinates/{http://datex2.eu/schema/1_0/1_0}longitude")[0].text,
            "to_lat" : location.findall(".//{http://datex2.eu/schema/1_0/1_0}to/{http://datex2.eu/schema/1_0/1_0}pointCoordinates/{http://datex2.eu/schema/1_0/1_0}latitude")[0].text,
            "to_lng" :  location.findall(".//{http://datex2.eu/schema/1_0/1_0}to/{http://datex2.eu/schema/1_0/1_0}pointCoordinates/{http://datex2.eu/schema/1_0/1_0}longitude")[0].text
            
          }
  return dict        
   
  
### take xml measurement and convert to python dictionary  
def measurement_to_dict(measurement, feed, pub_time):
  
  if feed == "http://www.traffcom.org/datex2/scootsettings/content.xml":
    
    ## initial metadata
    dict = {
      'site_ref' : measurement.findall("{http://datex2.eu/schema/1_0/1_0}measurementSiteReference")[0].text,
      'measurement_time' : parse(measurement.findall("{http://datex2.eu/schema/1_0/1_0}measurementTimeDefault")[0].text),
      'vehicle_flow' : int(measurement.findall(".//{http://datex2.eu/schema/1_0/1_0}vehicleFlow")[0].text),
      'concentration' : int(measurement.findall(".//{http://datex2.eu/schema/1_0/1_0}concentration")[0].text), 
      'pub_time' : pub_time
    }
 
  return dict



## iterate through measurements and insert into sql DB  
def measurements_to_postgres(measurements, pub_time, feed):
  
  ## Connect to the db
  conn, cur = db_connect(db_user, db)
  
  ## Create table if not exists TODO change to reflect many tables
  create_table("congestion", conn, cur)
  
  
  ## Get locations - removed while feed isn't working
  # root = get_feed_xml(feeds["scoot_definition"])
  # locations = get_measurements(root, feeds["scoot_definition"])[0]
  
  for measurement in measurements:
    
    try: 
      
      ## Convert measurement to dictionary TODO change to reflect many tables
      congestion = measurement_to_dict(measurement, feed, pub_time)
    
      ## append location information to measurement
      location = dict = {"from_description" :"", "to_description" :"", "from_lat" :0.0, "from_lng" :0.0, "to_lat" :0.0, "to_lng" : 0.0}
      #location = get_site_location(measurement, feeds["scoot_definition"], locations)
      congestion.update(location)
      
      ## Insert into the db TODO change to reflect many tables
      sql_string = "INSERT INTO congestion (site_ref, measurement_time, pub_time, vehicle_flow, concentration, from_description, to_description, from_lat, to_lat, from_lng, to_lng) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
      cur.execute(sql_string, (congestion["site_ref"], congestion["measurement_time"], congestion["pub_time"], congestion["vehicle_flow"], congestion["concentration"], congestion["from_description"], congestion["to_description"], congestion["from_lat"], congestion["to_lat"], congestion["from_lng"], congestion["to_lng"] ))
    
      ## commit the transaction
      conn.commit()
  
      ## TODO change to reflect many tables
      print "successfully inserted site %s reading from %s into the db" % (congestion["site_ref"], congestion["measurement_time"].strftime("%A, %d %B %Y : %H:%M:%S"))
    except:
      ## TODO change to reflect many tables
      print "Failed to insert site %s reading from %s into the db" % (congestion["site_ref"], congestion["measurement_time"].strftime("%A, %d %B %Y : %H:%M:%S"))
  
  ## close the connections
  cur.close()
  conn.close()

## connect to db  
def db_connect(user, database):
  conn = psycopg2.connect("dbname = %s user = %s " % (database, user))
  cur = conn.cursor()
  return conn, cur
  
## Reset table  
def reset_table(table_name):
  conn, cur = db_connect(db_user, db)
  
  ## delete db
  cur.execute("DROP TABLE %s" % (table_name))
  conn.commit()
  
  ## recreate db
  create_table(table_name, conn, cur)
  conn.commit()  

## Create the database
def create_table(table_name, conn, cur):
  
  if table_name == "congestion":
    cur.execute("create table if not exists congestion (id serial PRIMARY KEY, site_ref varchar(15) NOT NULL, measurement_time timestamp, pub_time timestamp, vehicle_flow int NOT NULL, concentration int NOT NULL, from_description varchar(255), to_description varchar(255), from_lat decimal, to_lat decimal, from_lng decimal, to_lng decimal);")
  
  if table_name == "logs":
    cur.execute("create table if not exists logs (id serial primary key, table_name varchar(15), run_time timestamp, pub_time timestamp, status varchar(15), log_message text)")
  conn.commit()
  return



## Write to log
def log_action(table, pub_time, status, message):
  
  ## Connect to database
  conn, cur = db_connect(db_user, db)
  
  ## Create if not exists
  create_table("logs", conn, cur)
  
  ## load in row
  cur.execute("INSERT INTO logs (run_time, table_name, pub_time, status, log_message) VALUES (%s, %s, %s, %s, %s)", (datetime.datetime.now(), table, pub_time, status, message))
  conn.commit()

## Main loop

def main():
  
  ## get measurements and publication time
  root = get_feed_xml(feeds["scoot_settings"])
  measurements, pub_time = get_measurements(root, feeds["scoot_settings"])
  
  ## connect to db
  conn, cur = db_connect(db_user, db)
  
  ## Create dbs if not already there
  create_table("congestion", conn, cur)
  create_table("logs", conn, cur)
  
  ## If no data in db, seed data first time TODO change to reflect many tables
  cur.execute("select * from congestion")
  if len(cur.fetchall()) == 0:
    measurements_to_postgres(measurements, pub_time, feeds["scoot_settings"])
    
  
  ## check if publication time is after the last item in the db and run TODO change to reflect many tables
  cur.execute("select pub_time from congestion order by pub_time desc limit 1")
  last_pub_time = cur.fetchone()[0]
  
  if pub_time > last_pub_time:
    try:
      measurements_to_postgres(measurements, pub_time, feeds["scoot_settings"])
      message = "Importing successful"
      log_action("congestion", pub_time, "SUCCESS", message)
    except:
      e = sys.exc_info()[0]
      message = "Updating failed: %s" % e
      log_action("congestion", pub_time, "FAILED", message)
  else:
    message = "read pub_date: %s, last pub_date: %s, so skipping" % (pub_time.strftime("%A, %d %B %Y : %H:%M:%S"), last_pub_time.strftime("%A, %d %B %Y : %H:%M:%S"))
    log_action("congestion", pub_time, "SKIPPED", message)

### Run script    
main()
    
    
