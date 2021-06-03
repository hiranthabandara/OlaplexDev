import psycopg2
import logging
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s  File:%(filename)s  Function:%(funcName)s()  Line:%(lineno)d  Msg: %(message)s'
)


class Redshift:

    def __init__(self, host, dbname, user, password, port=5439):
        self.db_config = {
            'host': host,
            'port': port,
            'dbname': dbname,
            'user': user,
            'password': password
        }
        self.logger = self._get_logger()
        self._set_up_connection(initial=True)  # open up the connection initially

    def _get_logger(self):
        logger = logging.getLogger(self.__str__())
        # modify logger based on your requirement here.
        return logger

    def _set_up_connection(self, initial=False):
        try:
            self.connection = psycopg2.connect(**self.db_config)
            if self._get_connection_status() == 'open':
                if initial:
                    self.logger.info("Database connection established.")
                else:
                    self.logger.info("Database connection re-established.")
        except Exception as e:
            self.logger.error("Couldn't connect to the database. Following exception occurred:")
            self.logger.error(e)
            raise

    def _get_connection_status(self):
        # connection.closed flag is zero if the connection is open and non-zero otherwise
        return 'open' if self.connection.closed == 0 else 'closed'

    def __str__(self):
        return 'Redshift:{}:{}/{}'.format(self.db_config['host'], self.db_config['port'], self.db_config['dbname'])

    def __del__(self):
        """
        In case the connection is left open(not recommended), it it closed here.
        """
        if self._get_connection_status() == 'open':
            self.connection.close()
            self.logger.info("Database connection closed.")

    def run_sql_command(self, sql_command, close_on_return=True):
        """
        Executes sql command on remote database server. DO NOT use this method to fetch data from server.
        Use this method only for the following cases:
            - COPY and UPLOAD commands
            - DDL commands such as CREATE, DROP, ALTER, TRUNCATE etc.
            - DML commands such as GRANT and REVOKE
        If close_on_return is set, connection is closed on return.
        """
        success = False
        # In case the connection is closed, set it up again
        if self._get_connection_status() == 'closed':
            self._set_up_connection()
        try:
            with self.connection.cursor() as cur:
                cur.execute(sql_command)
                self.connection.commit()
                success = True
        except Exception as e:
            self.logger.error(e)
            raise
        finally:
            if self._get_connection_status() == 'open' and close_on_return:
                self.connection.close()
                self.logger.info("Database connection closed.")
            return success

    def fetch_data(self, sql_command, batch_size=1000, close_on_return=True):
        """
        Creates a generator which fetches data returned by sql query in batches.
        This makes use of named cursor and has the potential to fetch large amount of data.
        """

        # In case the connection is closed, set it up again
        if self._get_connection_status() == 'closed':
            self._set_up_connection()
        try:
            # creating named cursor (server side cursor)
            # server side cursor allows fetching data in controlled manner without loading all the result in memory.
            # more details here: https://bit.ly/2Ci32tj
            with self.connection.cursor(name='cursor1') as cursor:
                cursor.execute(sql_command)
                while True:
                    # consume result over a series of iterations
                    # with each iteration fetching a batch of records
                    records = cursor.fetchmany(size=batch_size)
                    if not records:
                        break
                    else:
                        yield records

        except Exception as e:
            self.logger.error(e)
            raise

        finally:
            if self._get_connection_status() == 'open' and close_on_return:
                self.connection.close()
                self.logger.info("Database connection closed.")
            return

    def read_sql(self, sql, close_on_return=True, **kwargs):
        """
        Wrapper function around Pandas' read_sql()
        :param sql: sql command
        :param close_on_return: boolean indicating whether to close the database on return
        :param kwargs: all keyword args supported by pandas' read_sql function
        (https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_sql.html)
        :return: pandas dataframe with the result of the sql query
        """
        # In case the connection is closed, set it up again
        df = None
        if self._get_connection_status() == 'closed':
            self._set_up_connection()
        try:
            df = pd.read_sql(sql, self.connection, **kwargs)
        except Exception as e:
            self.logger.error(e)
            raise

        finally:
            if self._get_connection_status() == 'open' and close_on_return:
                self.connection.close()
                self.logger.info("Database connection closed.")
            return df
