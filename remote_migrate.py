import os
import wx
from datetime import datetime
import mysql.connector
import logging
import socket
import sshtunnel
import paramiko
from mysql.connector.locales.eng import client_error

def get_conn(remote):
    def wrapper_func(func):
        def inner_func(*args, **kwargs):
            # create mysql_tunnel()
            self = args[0]
            if remote:
                conn_details = self.remote_conn_details
            else:
                conn_details = self.local_conn_details

            logger = self.logger
            
            tunnel = connect_to_ssh(logger, conn_details)
            # create database connection()
            if tunnel:
                conn = connect_to_mysql(logger, tunnel, conn_details)
                result = func(conn, *args, **kwargs)
                conn.close()
                tunnel.close()

                return result
            else:
                return None
        return inner_func
    return wrapper_func




def connect_to_mysql(logger, tunnel, conn_details):
    try:
        conn = mysql.connector.MySQLConnection(
        host = '127.0.0.1',
        user = conn_details["database_user"],
        passwd = conn_details["database_password"],
        db = conn_details["database_name"],
        port=tunnel.local_bind_port,
        use_pure=True,
    )
    except Exception as e:
        
        logger.info(f"A connection error has occured")
    
    else:
        logger.info("Successfully connected to database")
        return conn

def connect_to_ssh(logger, conn_details, verbose = False):
    if verbose:
            sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG

        

    security_key = paramiko.RSAKey.from_private_key_file((os.getcwd() +f"\\keys\\{conn_details['ssh_key']}"), conn_details["ssh_password"]) 
    #security_key = paramiko.RSAKey.from_private_key_file(self.connection_details.security_filepath, self.connection_details.ssh_password)

    sshtunnel.SSH_TIMEOUT = 6000.0
    sshtunnel.TUNNEL_TIMEOUT = 12000.0

    tunnel = sshtunnel.SSHTunnelForwarder(
            (conn_details["ssh_ip"], conn_details["ssh_port"]),
            ssh_private_key=security_key,
            ssh_username = conn_details["ssh_username"],
            set_keepalive = 12000,
            remote_bind_address = ('127.0.0.1', 3306)
        )
    try:
            tunnel.start()
    except Exception as e:
        logger.error(f"A connection error has occured", str(e))
        frame = wx.Frame(None)
        wx.MessageBox("Error connecting to websites database", "Error!", wx.OK|wx.ICON_INFORMATION|wx.STAY_ON_TOP, frame, 120)
        frame.Destroy()
        return None
    else:
        return tunnel

        
         
    
def get_logger():
    d = datetime.now()

    log_file = d.strftime("%d %a-%m-%Y-%H-%M-%S")
    log_path = os.getcwd() + "\\logs"

    CHECK_FOLDER = os.path.isdir(log_path)
    if not CHECK_FOLDER:
          os.makedirs(log_path)

    print(log_path)


    logging.basicConfig(
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    datefmt="'%m/%d/%Y %I:%M:%S %p",
    handlers=[
        logging.FileHandler("{0}/{1}.log".format(log_path, log_file)),
        logging.StreamHandler()
    ],
    level = logging.DEBUG)

    logger = logging.getLogger(__name__)

    # get posta object
    logger.info("...Starting Application...")
    return logger


class DbMigrate(object):
    def __init__(self, logger = None):
        # set up local connection
        # set up remote connection
        # migrate database
        self.logger = logger or logging.getLogger(__name__)
        #self.local_ssh_connection()
        #self.remote_connection()

        self.remote_conn_details = {
            "ssh_ip": "162.213.251.237",
            "ssh_password": "incorrect0727531915",	
            "ssh_username" : "chuixkdt",
            "ssh_port": 21098,
            "ssh_key": "ggdbkeys",
            "database_user":"chuixkdt_crawls",
            "database_password": "incorrect1234",
            "database_name" : "chuixkdt_crawls"
        }

        self.local_conn_details = {
            "ssh_ip": "209.145.54.52",
            "ssh_password": "incorrect0727531915",
            "ssh_username" : "assignmentswrite",
            "ssh_port": 22,
            "ssh_key": "database_keys",
            "database_user":"assignmentswrite_crawls",
            "database_password": "incorrect0727531915",
            "database_name" : "assignmentswrite_crawls_b"
        }
    
        
    

    def local_connection(self):
        self.logger.info("Connecting to local db")
        self.local_conn = mysql.connector.connect(
        host="localhost",
        user="kush",
        password="incorrect",
        database="crawls"
        )
        
    def local_ssh_connection(self):
        self.logger.info("Connecting to local db")
        

        self.logger.info("Opening local ssh connection")
        self.local_tunnel = self.open_ssh_tunnel(self.local_conn_details)
        self.local_tunnel.start()
        self.logger.info("Connecting to local db")
        self.local_conn = self.open_db_connection(self.local_conn_details, self.local_tunnel)
        
        
    
    def remote_connection(self):
        self.logger.info("Connecting to remote db")
        
        self.logger.info("Opening remote ssh connection")
        self.remote_tunnel = self.open_ssh_tunnel(self.remote_conn_details)
        self.remote_tunnel.start()
        self.logger.info("Connecting to remote db")
        self.remote_conn = self.open_db_connection(self.remote_conn_details, self.remote_tunnel)

    def open_ssh_tunnel(self, conn_details, verbose=False):
        if verbose:
            sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG

        

        security_key = paramiko.RSAKey.from_private_key_file((os.getcwd() +f"\\keys\\{conn_details['ssh_key']}"), conn_details["ssh_password"]) 
        #security_key = paramiko.RSAKey.from_private_key_file(self.connection_details.security_filepath, self.connection_details.ssh_password)

        sshtunnel.SSH_TIMEOUT = 6000.0
        sshtunnel.TUNNEL_TIMEOUT = 12000.0

        tunnel = sshtunnel.SSHTunnelForwarder(
                (conn_details["ssh_ip"], conn_details["ssh_port"]),
                ssh_private_key=security_key,
                ssh_username = conn_details["ssh_username"],
                set_keepalive = 12000,
                remote_bind_address = ('127.0.0.1', 3306)
            )
            
        return tunnel

    def open_db_connection(self, conn_details, tunnel):
        try:
            conn = mysql.connector.MySQLConnection(
            host = '127.0.0.1',
            user = conn_details["database_user"],
            passwd = conn_details["database_password"],
            db = conn_details["database_name"],
            port=tunnel.local_bind_port,
            use_pure=True,
        )
        except Exception as e:
            self.connection_attempts = 0
            self.logger.info(f"A connection error has occured")
            if self.connection_attempts <= 3:
                self.logger.info(f"Connection attempt: {self.connection_attempts}")

                self.connection_attempts = self.connection_attempts + 1

                #self.open_ssh_tunnel(conn_details)
                #self.connect_to_mysql()
            else:
                self.logger.info(f"Unable to connect to db after multiple attempts")
        
            
        else:
            self.logger.info("Successfully connected to database")
            return conn

    def create_remote_tables(self):
        # start connections
        self.logger.info("Connecting to source databases to import table structure")
        self.local_ssh_connection()
        self.logger.info("Connecting to remote databases to import table structure")
        self.remote_connection()
        local_cursor = self.local_conn.cursor()

        # Get the table names from the local database
        #local_cursor.execute("SHOW TABLES LIKE '%_content'")
        
        local_cursor.execute("SHOW TABLES")
        tables = local_cursor.fetchall()

        remote_cursor = self.remote_conn.cursor()

        # Loop through each table and create it in the remote db
        for table in tables:
            create_table_query = f"SHOW CREATE TABLE {table[0]}"
            local_cursor.execute(create_table_query)
            create_table_statement = local_cursor.fetchone()[1]
            create_table_statement = create_table_statement.replace(self.local_conn.database, self.remote_conn.database)
        

            self.logger.info(create_table_statement)

            try:
                remote_cursor.execute(create_table_statement)
            except Exception as e:
                self.logger.error(e)
            else:
                self.logger.info(f"Table {table[0]} created in the remote db")

        remote_cursor.close()
        local_cursor.close()
        self.close_remote_conn()
        self.close_local_conn()


    def start_transfer(self):

        tables = self.fetch_tables()

      

        tables = [table['Tables_in_{}'.format(self.local_conn_details["database_name"])] for table in tables]
        #self.logger.info(tables)

        # Loop through each table
        for table in tables:
            # Get the column names from the local database
            columns = self.fetch_columns(table)

            #self.logger.info(f"{table}:{columns}")

            # Get the last exported record ID
            last_exported_id = 0

            # Number of records per chunk
            records_per_chunk = 500
            
            end = False

            # Generate the SQL statement to insert the records

            while not end:
                # Get the next chunk of records
                if "_links" in table:
                    query = f"SELECT * FROM {table} WHERE link_no > {last_exported_id} LIMIT {records_per_chunk}"
                else:
                    query = f"SELECT * FROM {table} WHERE no > {last_exported_id} LIMIT {records_per_chunk}"
                
                records = self.fetch_records(query)

                # Break the loop if there are no more records
                if not records:
                    end = True
                    break

                
                
                # Generate the insert query
                insert_query = "INSERT INTO {} ({}) VALUES ({})".format(
                    table,
                    ", ".join("`{}`".format(col) for col in columns),
                    ", ".join(["%s"] * len(columns))
                )

                self.logger.info(insert_query)

                last_exported_id = self.insert_records(insert_query, records, table)


                #self.logger.info(f"[SQL Query]: {insert_query}")

                # Execute the SQL statement
                
                self.logger.info(f"Import for batch {last_exported_id} successful")

        self.logger.info("Finishing execution and closing connections")
        # Close the connections
        self.local_conn.close()
        self.remote_conn.close()
        self.tunnel.close()


    
    @get_conn(remote = False)
    def fetch_tables(conn, self):
        # Get the table names from the local database
        local_cursor = conn.cursor(dictionary=True)
        #local_cursor.execute("SHOW TABLES LIKE '%_content'")
        local_cursor.execute("SHOW TABLES")

        return local_cursor.fetchall()
    
    @get_conn(remote = False)
    def fetch_columns(conn, self, table):
        local_cursor = conn.cursor(dictionary=True) 
        local_cursor.execute(f"DESCRIBE {table}")
        columns = [column['Field'] for column in local_cursor.fetchall()]
        return columns



    @get_conn(remote = True)
    def insert_records(conn, self, insert_query, records, table):
        try:
            #self.remote_conn.cursor().execute(sql)
            self.logger.info(records[1])

            conn.cursor().executemany(insert_query, [list(rec.values()) for rec in records])

            # Commit the changes
            conn.commit()
        except Exception as e:
            self.logger.error("Unable to update changes",str(e))
        else:
            # Update the last exported record ID
            if "_links" in table:
                last_exported_id = records[-1]['link_no']
            else:
                last_exported_id = records[-1]['no']
        return last_exported_id

    @get_conn(remote = False)
    def fetch_records(conn, self, query):
        local_cursor = conn.cursor(dictionary=True) 
        local_cursor.execute(query)
        return local_cursor.fetchall()
    
    def close_local_conn(self):
        self.local_conn.close()
        self.remote_tunnel.close()
    
    def close_remote_conn(self):
        self.remote_conn.close()
        self.remote_tunnel.close()






if __name__ == '__main__':

    logger = get_logger()
    
    app = DbMigrate(logger)

    app.create_remote_tables()
    
    app.start_transfer()

