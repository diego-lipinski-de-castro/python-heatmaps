import sys
import pandas as pd
from folium import Map
from folium.plugins import HeatMap
import psycopg2

param_dic = {
    "host"      : "",
    "database"  : "",
    "user"      : "",
    "password"  : ""
}

def connect(params):
    try:
        print('Connecting to the PostgreSQL database...')
        c = psycopg2.connect(**params)
        print("Connection successful")
        return c
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1) 

def close(c):
    try:
        print('Closing connection to the PostgreSQL database...')
        c.close()
        print('Connection closed')
    except (Exception) as error:
        print(error)
        sys.exit(1) 

def postgresql_to_dataframe(c, select_query, column_names):
    cursor = c.cursor()

    try:
        cursor.execute(select_query)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cursor.close()
        return 1
    
    tupples = cursor.fetchall()
    cursor.close()
    
    df = pd.DataFrame(tupples, columns=column_names)
    return df

def run():
    conn = connect(param_dic)

    # query to get the latitude and longitude
    
    # you can either use the same query as mysql if using double columns
    # query = 'select lat as y, lng as x from table'
    # column_names = ['y', 'x']

    # or you can use this way if youre using postgis columns datatype
    query = 'select st_x(location::geometry) as x, st_y(location::geometry) as y from table'
    column_names = ['x', 'y']

    df = postgresql_to_dataframe(conn, query, column_names)
    df.head()

    # set center of the map with lng, lat
    hmap = Map(location=[0, 0], zoom_start=12)

    hm_wide = HeatMap(list(zip(df.y.values, df.x.values)))

    hmap.add_child(hm_wide)

    hmap.save("generated/heatmap.html")

    close(conn)
    conn = None

# call run to generate the html heatmap
run()