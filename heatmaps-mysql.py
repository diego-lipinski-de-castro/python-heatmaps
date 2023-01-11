import sys
import pandas as pd
from folium import Map
from folium.plugins import HeatMap
import mariadb

param_dic = {
    "host"      : "",
    "database"  : "",
    "user"      : "",
    "password"  : ""
}

def connect(params):
    try:
        print('Connecting to the MySQL database...')
        c = mariadb.connect(**params)
        print("Connection successful")
        return c
    except (Exception, mariadb.Error) as error:
        print(error)
        sys.exit(1)

def close(c):
    try:
        print('Closing connection to the MySQL database...')
        c.close()
        print('Connection closed')
    except (Exception) as error:
        print(error)
        sys.exit(1) 

def mysql_to_dataframe(c, select_query, column_names):
    cursor = c.cursor()

    try:
        cursor.execute(select_query)
    except (Exception, mariadb.Error) as error:
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
    query = 'select lat as y, lng as x from table'
    column_names = ['y', 'x']

    df = mysql_to_dataframe(conn, query, column_names)
    df.head()

    # set center of the map with lng, lat
    hmap = Map(location=[0, 0], zoom_start=12)

    hm_wide = HeatMap(list(zip(df.y.values, df.x.values)))

    hmap.add_child(hm_wide)

    hmap.save("heatmap.html")

    close(conn)
    conn = None

# call run to generate the html heatmap
run()