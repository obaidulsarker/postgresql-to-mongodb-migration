#from pymongo import MongoClient, ASCENDING, DESCENDING
import psycopg2
import psycopg2.extras
from psycopg2 import OperationalError
from setting import get_variables
from logger import *
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
# import warnings
# warnings.filterwarnings("ignore", category=UserWarning)

class PostgresDatabase(Logger):
    def __init__(self, logfile):
        super().__init__(logfile)

        # TARGET DB [PG]
        self.pg_host = get_variables().PG_HOST
        self.pg_port = get_variables().PG_PORT
        self.pg_database = get_variables().PG_DATABASE
        self.pg_username= get_variables().PG_USER
        self.pg_password = get_variables().PG_PASSWORD
        self.pg_schema = get_variables().PG_SCHEMA

        # ETL DB [PG]
        self.etl_pg_host = get_variables().ETL_PG_HOST
        self.etl_pg_port = get_variables().ETL_PG_PORT
        self.etl_pg_database = get_variables().ETL_PG_DATABASE
        self.etl_pg_username= get_variables().ETL_PG_USER
        self.etl_pg_password = get_variables().ETL_PG_PASSWORD
        self.etl_pg_schema = get_variables().ETL_PG_SCHEMA
        self.etl_pg_table = get_variables().ETL_PG_TABLE
      
    # PostgreSQL connection
    def pg_connect(self):
        try:
            connection = psycopg2.connect(
                host=self.pg_host,
                port=self.pg_port,
                user=self.pg_username,
                password=self.pg_password,
                dbname=self.pg_database,
                connect_timeout = 3600
            )
            
            connection.autocommit=True

            return connection  # Success
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error
    
    # Disconnect from PG
    def pg_disconnect(self):
        if self.pg_connect():
            self.pg_connect().close()

    def pg_etl_connect(self):
        try:
            connection = psycopg2.connect(
                host=self.etl_pg_host,
                port=self.etl_pg_port,
                user=self.etl_pg_username,
                password=self.etl_pg_password,
                dbname=self.etl_pg_database,
                connect_timeout = 3600
            )
            
            connection.autocommit=True

            return connection  # Success
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error
    
    # Disconnect from PG
    def pg_etl_disconnect(self):
        if self.pg_etl_connect():
            self.pg_etl_connect().close()

    # INSERT ETL record
    def pg_ins_etl_table(self, data):
        try:
            last_sync_timestamp = datetime.now() - relativedelta(years=100)

            sql=f""" INSERT INTO {self.etl_pg_schema}.{self.etl_pg_table}(
	trg_table_name, src_index_name, src_pk_col_name, src_timestamp_col_name, last_sync_timestamp, is_active)
    VALUES('{data.collection_name}','{data.collection_name}','{data.id_field_name}','{data.ts_field_name}','{last_sync_timestamp}',TRUE);
                """

            status = self.pg_execute_dml(query=sql, value=None)
            if (status is None):
                raise Exception(f"Unable to initialize the ETL table for {data.collection_name} collection.")

            return status

        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error

    # Get ETL mapping
    def pg_get_etl_table(self):
        try:
            query=f"""SELECT trg_table_name, src_index_name, src_pk_col_name, src_timestamp_col_name, last_sync_timestamp
                FROM {self.etl_pg_schema}.{self.etl_pg_table}
                WHERE is_active IS TRUE
                ORDER BY trg_table_name"""

            self.log_info(f"Executing : {query}")
            print(f"Executing : {query}")

            conn = self.pg_etl_connect()
            df = pd.read_sql(query, conn)

            #self.connection.commit()
            conn.close()
            self.log_info(f"Executed : {query}")
            print(f"Executed : {query}")
            return df  # Success
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error

    # Fetch Last Sync DateTime
    def get_last_sync_time(self, pg_table_name):
        try:
            query = f""" SELECT last_sync_timestamp FROM "{self.etl_pg_schema}"."{self.etl_pg_table}" 
            WHERE lower(src_table_name)=lower('{pg_table_name}') """
            
            print(f"Executing : {query}")
            self.log_info(f"Executing : {query}")

            cursor = self.pg_etl_connect().cursor()
            cursor.execute(query=query)
            result = cursor.fetchone()

            self.log_info(f"Executed : {query}")
            print(f"Executed : {query}")

            return result[0] if result else None

        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return  (datetime.now() - relativedelta(years=100))  # Error
        
    # Update last sync datetime
    def update_last_sync_time(self, pg_table_name, last_sync_time):
        try:
            query=f""" UPDATE {self.etl_pg_schema}.{self.etl_pg_table} SET
            last_sync_timestamp='{last_sync_time}'
            WHERE src_table_name='{pg_table_name}' 
            """
            print(f"Executing : {query}")
            self.log_info(f"Executing : {query}")

            cursor = self.pg_etl_connect().cursor()
            cursor.execute(query=query)
            cursor.close()

            self.log_info(f"Executed : {query}")
            print(f"Executed : {query}")

            return True
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error
        
    # Execute PG DDL Statement
    def pg_execute_ddl(self, query):
        try:
            print(f"Executing : {query}")
            self.log_info(f"Executing : {query}")
            cursor = self.pg_connect().cursor()
            cursor.execute(query)
            #self.connection.commit()
            cursor.close()
            self.log_info(f"Executed : {query}")
            print(f"Executed : {query}")
            return True  # Success
        
        except OperationalError as e:
            print(f"DDL Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return False  # Error
    
    def pg_execute_dml(self, query, value):
        try:
            #self.log_info(f"Executing : {query}")
            #print(f"Executing : {query}")
            cursor = self.pg_connect().cursor()
            if value is None:
                cursor.execute(query)
            else:
                cursor.execute(query, value)

            #self.connection.commit()
            cursor.close()
            #self.log_info(f"Executed : {query}")
            #print(f"Executed : {query}")
            return True  # Success
        
        except OperationalError as e:
            cursor.close()
            print(f"DML Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return False  # Error
    
    def pg_execute_many_dml(self, query, value):
        try:
            self.log_info(f"Executing : {query}")
            print(f"Executing : {query}")

            # self.log_info(f"value : {value}")
            # print(f"value : {value}")

            cursor = self.pg_connect().cursor()
            cursor.executemany(query, value)
            self.pg_connect().commit()

            cursor.close()

            return True  # Success
        
        except OperationalError as e:
            cursor.close()
            print(f"DML Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return False  # Error
        
    def pg_execute_dml_return_rowcount(self, query, value):
        try:
            #self.log_info(f"Executing : {query}")
            #print(f"Executing : {query}")
            cursor = self.pg_connect().cursor()
            if value is None:
                cursor.execute(query)
            else:
                cursor.execute(query, value)

            #self.connection.commit()
            cursor.close()
            #self.log_info(f"Executed : {query}")
            #print(f"Executed : {query}")
            return cursor.rowcount  # Success
        
        except OperationalError as e:
            cursor.close()
            print(f"DML Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return 0  # Error
        
    # Get records of SELECT query
    def pg_get_df(self, query):
        try:
            self.log_info(f"Executing : {query}")
            print(f"Executing : {query}")

            conn = self.pg_connect()
            df = pd.read_sql(query, conn)

            #self.connection.commit()
            conn.close()
            self.log_info(f"Executed : {query}")
            print(f"Executed : {query}")
            return df  # Success
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error
    
    # Get PostgreSQL Table's Column in Dataframe
    def get_table_columns(self, table_name):
        try:
            sql=f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' and table_schema='{self.pg_schema}' AND column_name<>'id';"
            df = self.pg_get_df(query=sql)
            return df
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error

    def check_index_existance(self, table_schema, table_name, column_name):
        try:
            exists=False

            # Step 1: Check if column is part of any index
            check_index_query = f"""
            SELECT 1 as cnt
            FROM pg_index i
            JOIN pg_class c ON c.oid = i.indrelid
            JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = '{table_schema}'
            AND c.relname = '{table_name}'
            AND a.attname = '{column_name}';
            """

            df = self.pg_get_df(query=check_index_query)

            indx_count=0
            if df[df.columns[0]].count()>0:
                indx_count = df['cnt']
                print(f"indx_count={indx_count}")
                exists = True
     
            return exists
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error

    def create_index(self, table_schema, table_name, column_name):
        try:

            index_name = f"idx_{table_name}_{column_name}".lower()

            create_index_query = f"""
            CREATE INDEX IF NOT EXISTS {index_name}
            ON  "{table_schema}"."{table_name}"("{column_name}");
            """

            create_status=False
            # Create index if not exist
            if self.check_index_existance(table_schema=table_schema, table_name=table_name, column_name=column_name)==False:
                create_status = self.pg_execute_ddl(query=create_index_query)
            else:
                create_status=True

            return create_status
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error

    def is_validate_fields(self, schema_name, table_name, fields_str):
        try:
            is_valid = False

            # Validate fileds
            invalid_fields = self.validate_fields(schema_name=schema_name, table_name=table_name, fields_str=fields_str)

            if len(invalid_fields)>0:
                is_valid = False
                raise Exception(f"Invalid Fields in {table_name}: {self.list_to_comma_string(invalid_fields)}")
            else:
                is_valid = True

            return is_valid

        except Exception as e:
            print(f"An error occurred: {e}")
            self.log_error(f"An error occurred: {e}")
            return is_valid

    # Validate fields
    def validate_fields(self, schema_name, table_name, fields_str):
        """
        Validates the existence of a list of fields in a MongoDB collection.

        Args:
            schema_name (str): The name of the schema.
            table_name (str): The name of the table.
            fields_str (str): A comma-separated string of field names to validate.
            

        Returns:
            list: A list of invalid field names, or an empty list if all fields are valid.
        """
        try:
           
            sql=f"""SELECT * FROM "{schema_name}"."{table_name}" LIMIT 1;"""

            df = self.pg_get_df(query=sql)
            column_list = df.columns.tolist()
            column_list = [item.strip() for item in column_list]
  
            print(f"column_list={column_list}")

            fields = [field.strip() for field in fields_str.split(',')]
            fields = [item.strip(' "').strip('" ') for item in fields] # Remove double quote

            invalid_fields = []

            for field in fields:
                if field not in column_list:
                    invalid_fields.append(field)

            print(f"invalid_fields={invalid_fields}")

            return invalid_fields

        except Exception as e:
            print(f"An error occurred: {e}")
            self.log_error(f"An error occurred: {e}")
            return fields # return all fields in case of error.
    
    def list_to_comma_string(self, input_list):
        """Converts a list into a comma-separated string.

        Args:
            input_list: The list to convert.

        Returns:
            A string with the list elements separated by commas.
        """
        return ', '.join(map(str, input_list))
    
    def get_minDate_maxDate_date_total_records(self, schema_name, table_name, created_ts_column, updated_ts_column, from_date):
        try:
            max_date = None
            min_date = from_date
            total_records = 0

            sql=f""" SELECT MIN("{created_ts_column}") as min_date, MAX("{created_ts_column}") as max_date, COUNT(1) AS total_records
                    FROM "{schema_name}"."{table_name}" 
                    WHERE ("{created_ts_column}">= '{from_date}' OR "{updated_ts_column}"<'{from_date}')
                           
                """

            df= self.pg_get_df(sql)

            min_date=df['min_date'].values[0] if len(df) > 0 else None
            max_date=df['max_date'].values[0] if len(df) > 0 else None
            total_records = df["total_records"].values[0] if len(df) > 0 else 0

            return min_date, max_date, total_records
        
        except Exception as e:
            print(f"An error occurred: {e}")
            self.log_error(f"An error occurred: {e}")
            return None, None, None

    # Map Pandas Dataframe to PostgreSQL
    def _map_pandas_dtype_to_pg(self, dtype):
        """Maps Pandas data types to PostgreSQL data types."""
        if dtype == 'int64':
            return 'BIGINT'
        elif dtype == 'float64':
            return 'DOUBLE PRECISION'
        elif dtype == 'datetime64[ns]':
            return 'TIMESTAMP WITH TIME ZONE'
        elif dtype == 'object':
            return 'TEXT'
        else:
            return 'TEXT'
    
    # Migrate Data
    def migrate_data(self, mongo_df, table_name, unique_column="_id"):
        try:

            # print(mongo_df.dtypes)
            # self.log_info(mongo_df.dtypes)

            # Get PostgreSQL Table Column List
            pg_table_columns_df = self.get_table_columns(table_name=table_name)
            existing_columns = [row[0] for row in pg_table_columns_df]


            # Construct MERGE (UPSERT) query
            columns_str = ', '.join([f'"{col}"' for col in mongo_df.columns])
            placeholders = ', '.join(['%s'] * len(mongo_df.columns))
            update_assignments = ', '.join([f'"{col}" = EXCLUDED."{col}"' for col in mongo_df.columns if col != unique_column])

            merge_query = f"""
                INSERT INTO {self.pg_schema}."{table_name}" ({columns_str}) VALUES ({placeholders})
                ON CONFLICT ("{unique_column}")
                DO UPDATE SET {update_assignments};
            """
            #print(merge_query)
            
            # Convert DataFrame to list of tuples for psycopg2
            data_tuples = [tuple(x) for x in mongo_df.to_numpy()]
            status = self.pg_execute_many_dml(query=merge_query, value=data_tuples)

            return status
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error
        

    # Alter Tables for new columns
    def alter_table_for_new_columns(self, mongo_df, table_name):
        try:
            alter_status = False

            columns_df = self.get_table_columns(table_name=table_name)

            existing_columns = [row[0] for row in columns_df]
            for col_name, dtype in mongo_df.dtypes.items():
                if col_name not in existing_columns:
                    pg_type = self._map_pandas_dtype_to_pg(dtype)
                    alter_query = f'ALTER TABLE "{table_name}" ADD COLUMN "{col_name}" {pg_type}'
                    alter_status = self.pg_execute_ddl(alter_query)

            return alter_status
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error
        
    # Create Table based on MongoDB Collection
    def create_table_if_not_exists(self, df, collection_info):
        try:
            unique_column=collection_info.id_field_name
            table_name = collection_info.collection_name
            columns = []
            for col_name, dtype in df.dtypes.items():
                if dtype == 'int64':
                    pg_type = 'BIGINT'
                elif dtype == 'float64':
                    pg_type = 'DOUBLE PRECISION'
                elif dtype == 'datetime64[ns]':
                    pg_type = 'TIMESTAMP WITH TIME ZONE'
                    df[col_name] = df[col_name].apply(lambda x: x.isoformat() if pd.notnull(x) else None) #convert to iso format string.
                elif dtype == 'object':
                    pg_type = 'TEXT'
                else:
                    pg_type = 'TEXT' # Default to TEXT for other types
                columns.append(f'"{col_name}" {pg_type}')
            
            create_table_query = f'CREATE TABLE IF NOT EXISTS  {self.pg_schema}."{table_name}" (id BIGSERIAL NOT NULL PRIMARY KEY, {", ".join(columns)}, UNIQUE ({unique_column}))'

            is_created_table = self.pg_execute_ddl(query=create_table_query)

            if is_created_table:
                print(f"Table Created : {table_name}")
                self.log_info(f"Table Created : {table_name}")
                
                # Insert ETL records
                is_etl_initialized=self.pg_ins_etl_table(data=collection_info)


            return is_created_table

        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error
    
        
    # Create Primary key if does not exists
    def create_primary_key(self, table_name, column_name):
        try:
            query = f"""
                SELECT 1 as cnt
                FROM information_schema.table_constraints tc
                JOIN information_schema.constraint_column_usage ccu
                ON tc.constraint_name = ccu.constraint_name
                WHERE tc.table_name = '{table_name}'
                AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
                AND ccu.column_name = '{column_name}'
            ;"""
            df = self.pg_get_df(query=query)
            record_count = len(df)


            print(f"Record count: {record_count}")

            # Check primary key column is in UK
            if (record_count<1):

                # Check whether unique key index is existed or not
                index_name=f"uk_{table_name}_{column_name}"
                sql_uk_existance=f"""
                    SELECT 1 AS cnt
                    FROM pg_indexes
                    WHERE tablename = '{table_name}'
                    AND indexname = '{index_name}';
                    """
                
                df_uk = self.pg_get_df(query=sql_uk_existance)
                record_count_uk = len(df_uk)

                if(record_count_uk<1):
                    alter_query = f'ALTER TABLE {self.pg_schema}.{table_name} ADD CONSTRAINT {index_name} UNIQUE ("{column_name}");'
                    result = self.pg_execute_ddl(query=alter_query)

                    if (result):
                        print(f"Primary Key constraints is created on {table_name} using {column_name}.")
                        self.log_info(f"Primary Key constraints is created on {table_name} using {column_name}.")
                    else:
                        raise Exception(f"Unable to create primary key constraints on {table_name} using {column_name}.")

            return True
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error
    