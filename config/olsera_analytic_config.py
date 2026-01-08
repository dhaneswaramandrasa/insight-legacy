import logging
import sshtunnel
from sshtunnel import SSHTunnelForwarder
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd

load_dotenv()
host = os.getenv("ANALITYC_HOST")
ssh_host = os.getenv("SSH_HOST")
ssh_username = os.getenv("SSH_USERNAME")
database_username = os.getenv("DATABASE_USERNAME_ANALITYC")
database_password = os.getenv("DATABASE_PASSWORD_ANALITYC")
database_name = os.getenv("DATABASE_NAME_ANALITYC")
localhost = os.getenv("LOCALHOST")
port = 3306

# Open SSH tunnel
def open_ssh_tunnel(verbose=False):
    if verbose:
        sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG
    
    global tunnel
    tunnel = SSHTunnelForwarder(
        (ssh_host, 22),
        ssh_username=ssh_username,
        ssh_pkey="ssh_db_replicate_live.pem",  # Path to your private key
        remote_bind_address=(host, 3306)
    )
    
    tunnel.start()

# open_ssh_tunnel()

# Create a connection string using SQLAlchemy
def get_sqlalchemy_engine():
    # connection_string = f"mysql+pymysql://{database_username}:{database_password}@{localhost}:{tunnel.local_bind_port}/{database_name}"
    connection_string = f"mysql+pymysql://{database_username}:{database_password}@{host}:{port}/{database_name}"
    engine = create_engine(connection_string)
    return engine