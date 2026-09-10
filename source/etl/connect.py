import psycopg2 

def connectDB():
    conn = psycopg2.connect(
        host = "localhost",
        port = 5432,
        database = "bus_delay",
        user = "bus_admin",
        password = "buspassword"
    )
    return conn