import os
from datetime import datetime
import mysql.connector
import logging
import socket
import sshtunnel
import paramiko
from mysql.connector.locales.eng import client_error


class DbMigrate(object):
    def __init__(self, logger = None):
        # set up local connection
        # set up remote connection
        # migrate database
        self.logger = logger or logging.getLogger(__name__)
        self.local_connection()
        self.remote_connection()
    
        
    

    def local_connection(self):
        self.logger.info("Connecting to local db")
        self.local_conn = mysql.connector.connect(
        host="localhost",
        user="kush",
        password="incorrect",
        database="crawls"
        )

        
    
    def remote_connection(self):
        self.logger.info("Connecting to remote db")
        self.open_ssh_tunnel()
        self.open_db_connection()

    def open_ssh_tunnel(self, verbose=False):
        if verbose:
            sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG

        ssh_password = "incorrect0727531915"


        security_key = paramiko.RSAKey.from_private_key_file((os.getcwd() +"\\keys\\database_key"),ssh_password) 
        #security_key = paramiko.RSAKey.from_private_key_file(self.connection_details.security_filepath, self.connection_details.ssh_password)

        sshtunnel.SSH_TIMEOUT = 6000.0
        sshtunnel.TUNNEL_TIMEOUT = 12000.0

        self.tunnel = sshtunnel.SSHTunnelForwarder(
                ('198.54.115.176', 21098),
                ssh_private_key=security_key,
                ssh_username = 'examwfgd',
                set_keepalive = 12000,
                remote_bind_address = ('127.0.0.1', 3306)
            )
            
        self.tunnel.start()

    def open_db_connection(self):
        try:
            self.remote_conn = mysql.connector.MySQLConnection(
            host = '127.0.0.1',
            user = 'examwfgd_crawls',
            passwd = 'incorrect0727531915',
            db = 'examwfgd_crawls',
            port=self.tunnel.local_bind_port,
            use_pure=True,
        )
        except Exception as e:
            self.connection_attempts = 0
            self.logger.info(f"A connection error has occured")
            if self.connection_attempts <= 3:
                self.logger.info(f"Connection attempt: {self.connection_attempts}")

                self.connection_attempts = self.connection_attempts + 1

                self.open_ssh_tunnel()
                self.connect_to_mysql()
            else:
                self.logger.info(f"Unable to connect to db after multiple attempts")
        
            
        else:
            self.logger.info("Successfully connected to remote database")

    def create_remote_tables(self):
        local_cursor = self.local_conn.cursor()

        # Get the table names from the local database
        local_cursor.execute("SHOW TABLES LIKE '%_content'")
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


    def start_transfer(self):

        # Get the table names from the local database
        local_cursor = self.local_conn.cursor(dictionary=True)
        local_cursor.execute("SHOW TABLES LIKE '%_content'")
        
        #self.logger.info(tables)
        tables = [table['Tables_in_{} (%_content)'.format(self.local_conn.database)] for table in local_cursor.fetchall()]
        #self.logger.info(tables)

        # Loop through each table
        for table in tables:
            # Get the column names from the local database
            local_cursor.execute(f"DESCRIBE {table}")
            columns = [column['Field'] for column in local_cursor.fetchall()]

            #self.logger.info(f"{table}:{columns}")

            # Get the last exported record ID
            last_exported_id = 0

            # Number of records per chunk
            records_per_chunk = 1000
            
            end = False

            # Generate the SQL statement to insert the records
            """
            num_cols_query = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{}';".format(table[0])
            try:
                num_cols = local_cursor.execute(num_cols_query).fetchone()[0]
            except Exception as e:
                self.logger.error(e)"""

            while not end:
                # Get the next chunk of records
                local_cursor.execute(f"SELECT * FROM {table} WHERE no > {last_exported_id} LIMIT {records_per_chunk}")
                records = local_cursor.fetchall()

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

                


                #self.logger.info(f"[SQL Query]: {insert_query}")

                # Execute the SQL statement
                try:
                    #self.remote_conn.cursor().execute(sql)
                    self.logger.info(records[1])

                    self.remote_conn.cursor().executemany(insert_query, [list(rec.values()) for rec in records])

                    # Commit the changes
                    self.remote_conn.commit()
                except Exception as e:
                    self.logger.error("Unable to update changes",str(e))
                else:
                    # Update the last exported record ID
                    last_exported_id = records[-1]['no']
                    self.logger.info(f"Import for batch {last_exported_id} successful")

        self.logger.info("Finishing execution and closing connections")
        # Close the connections
        self.local_conn.close()
        self.remote_conn.close()
        self.tunnel.close()

        
         
    
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



if __name__ == '__main__':

    logger = get_logger()
    
    app = DbMigrate(logger)

    app.create_remote_tables()
    
    app.start_transfer()

