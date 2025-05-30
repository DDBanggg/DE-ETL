from databases.mysql_connect import MySQLConnect
from config.database_config import get_database_config

def main(config):
    with MySQLConnect(config["mysql"].host, config["mysql"].port, config["mysql"].user, config["mysql"].password) as mysql_client:
        connection = mysql_client.connection
        cursor = mysql_client.cursor

if __name__ == "__main__":
    config = get_database_config()
    main(config)