import configparser  # For reading configuration files
from sqlalchemy import create_engine  # For creating a connection to the database
import sqlalchemy  # General SQLAlchemy package for database operations
import urllib.parse  # For encoding special characters in the password
import os  # For accessing file paths and system-level operations

# Get the directory path of the current file
dir_path = os.path.dirname(os.path.realpath(__file__))

# Initialize and read the configuration file
config = configparser.ConfigParser()
config.read(f"{dir_path}/config.ini")  # The configuration file should be in the same directory as this script

def db_conn(conn_param):
    """
    Establishes a connection to a PostgreSQL database using parameters defined in a configuration file.

    Parameters
    ----------
    conn_param : str
        Section name in the `config.ini` file that contains database connection parameters.
        Expected keys within the section:
            - POSTGRES_ADDRESS: Address of the PostgreSQL server (e.g., 'localhost' or cloud DB link)
            - POSTGRES_PORT: Port number for PostgreSQL server (e.g., 5432)
            - POSTGRES_USERNAME: Username for database authentication
            - POSTGRES_PASSWORD: Password for database authentication
            - POSTGRES_DBNAME: Name of the database to connect to

    Returns
    -------
    tuple
        A tuple containing:
            - engine: SQLAlchemy Engine object for database operations
            - conn: Connection object with streaming results enabled
        If the connection fails, both `engine` and `conn` will be `None`.
    """
    # Extract database connection parameters from the config file
    POSTGRES_ADDRESS = config[conn_param]['POSTGRES_ADDRESS']  # Address of the PostgreSQL server
    POSTGRES_PORT = config[conn_param]['POSTGRES_PORT']  # Port for the PostgreSQL server
    POSTGRES_USERNAME = config[conn_param]['POSTGRES_USERNAME']  # Database username
    # Encode the password to handle special characters (e.g., '@', '&')
    POSTGRES_PASSWORD = urllib.parse.quote_plus(config[conn_param]['POSTGRES_PASSWORD'])
    POSTGRES_DBNAME = config[conn_param]['POSTGRES_DBNAME']  # Name of the target database

    # Build the connection string using the extracted parameters
    conn_str = f'postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_ADDRESS}:{POSTGRES_PORT}/{POSTGRES_DBNAME}'

    try:
        # Create the SQLAlchemy engine using the connection string
        engine = create_engine(conn_str)

        # Establish a connection with streaming results enabled
        conn = engine.connect().execution_options(stream_results=True)
    except (sqlalchemy.exc.DBAPIError, sqlalchemy.exc.InterfaceError) as err:
        # Handle connection errors and print a descriptive message
        print('Database could not connect\n', err)
        # Set engine and conn to None to signify failure
        engine = None
        conn = None
    finally:
        # Return the engine and connection object, even if they are None
        return engine, conn