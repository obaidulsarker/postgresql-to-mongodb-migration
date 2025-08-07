from PostgresqlDB import *
from MongoDatabase import *
from xml_reader import *
import pandas as pd

from logger import Logger

class ETL(Logger):
    def __init__(self, logfile, operation_id):
        super().__init__(logfile)
        self.operation_log=logfile
        self.operation_id = operation_id
        self.batch_size = get_variables().BATCH_SIZE

    # Doing automation tasks
    def start_jobs(self):
        try:
        
            operation_log = self.operation_log

            # Read all collections configuration info
            xml = XmlReader(logfile=operation_log)

            # PostgreSQL Instance
            pg = PostgresDatabase(logfile=operation_log)

            # MongoDB instance
            mongo = MongoDatabase(logfile=operation_log)
            
            table_list = xml.get_collection_list()

            if table_list is None:
                raise Exception("No table is found in configuration file.")

            # Loop through collection
            for item in table_list:
                try:

                    # Set Value
                    pg_schema_name=item.pg_schema_name
                    mongo_schema_name=item.mongo_schema_name
                    table_name=item.table_name
                    fields_str=item.fields_list
                    unique_column=item.id_field_name
                    created_ts_field_name=item.created_ts_field_name
                    updated_ts_field_name=item.updated_ts_field_name
                    mongo_table_name=item.mongo_table_name
                    selected_columns_list=item.selected_columns_list
                    column_field_mapping=item.column_field_mapping

                    # get last sync timestamp of the collection from ETL table
                    last_sync_ts=pg.get_last_sync_time(pg_table_name=table_name)
                    if last_sync_ts is None:
                        last_sync_ts=datetime.now() - relativedelta(years=100)

                    str_last_sync_ts=last_sync_ts.strftime("%Y-%m-%d")

                    print(f"last sync date: {str_last_sync_ts}")
                    self.log_info(f"last sync date: {str_last_sync_ts}")

                    index_status = pg.create_index(table_schema=pg_schema_name, table_name=table_name, column_name=created_ts_field_name)
                    if not index_status:
                        raise Exception(f"Unable to create index on {table_name} table for {created_ts_field_name} column.")

                    index_status = pg.create_index(table_schema=pg_schema_name, table_name=table_name, column_name=updated_ts_field_name)
                    if not index_status:
                        raise Exception(f"Unable to create index on {table_name} table for {updated_ts_field_name} column.")
                    
                    # Validate MongoDB Collection Fields
                    has_valid_fields = pg.is_validate_fields(schema_name=pg_schema_name, table_name=table_name,  fields_str=fields_str)
                    if (has_valid_fields==False):
                        raise Exception(f"Invalid fields found in configuration for the {table_name} table.")

                    # Find Minimum and Maximum date of Timestamp column and total records
                    minimum_datetime, maximum_datetime, total_records = pg.get_minDate_maxDate_date_total_records(schema_name=pg_schema_name, table_name=table_name, created_ts_column=created_ts_field_name, updated_ts_column=updated_ts_field_name, from_date=last_sync_ts)
                    
                    print(f"minimum_datetime: {type(minimum_datetime)}")
                    self.log_info(f"minimum_datetime: {type(minimum_datetime)}")

                    print(f"Minimum value of {created_ts_field_name} field on {table_name}: {minimum_datetime}")
                    print(f"Maximum value of {created_ts_field_name} field on {table_name}: {maximum_datetime}")
                    print(f"Total records for synchronization of {table_name}: {total_records}")
                    
                    self.log_info(f"Minimum value of {created_ts_field_name} field on {table_name}: {minimum_datetime}")
                    self.log_info(f"Maximum value of {created_ts_field_name} field on {table_name}: {maximum_datetime}")
                    self.log_info(f"Total records for synchronization of {table_name}: {total_records}")

                    # # Get dataframe of MongoDB
                    # sample_mongo_data = mongo.get_dataframe(database_name=database_name, collection_name=collection_name, fields_str=fields_str, timestamp_column=timestamp_column, from_date=minimum_datetime, to_date=maximum_datetime, row_limit=500)
                    
                    # if (sample_mongo_data.empty):
                    #     raise Exception(f"No record found in {collection_name}")

                    # # Create PostgreSQL Table
                    # table_created= pg.create_table_if_not_exists(df=sample_mongo_data, collection_info=item)

                    # if table_created is None:
                    #     raise Exception(f"Unable to create {collection_name}")
                    

                    # # Add new columns in PostgreSQL
                    # created_new_columns= pg.alter_table_for_new_columns(mongo_df=mongo_df.head(), table_name=item.collection_name)

                    # if created_new_columns is None:
                    #     raise Exception(f"Unable alter table {item.collection_name}")
                    
                    # from_date = truncate(minimum_datetime, 'day')
                    # to_date = truncate(maximum_datetime, 'day')

                    from_date =  minimum_datetime.astype('datetime64[D]').astype(object)
                    to_date =  maximum_datetime.astype('datetime64[D]').astype(object)

                    print(f"from_date: {type(from_date)}")
                    self.log_info(f"from_date: {type(from_date)}")


                    record_count = 0
                    batch_record_count= 0

                    print("********************************")
                    self.log_info("***************************")
                    while (from_date<=to_date):
                        start_date = from_date
                        end_date = start_date + timedelta(days=1)
                        batch_record_count=0

                        # Next Date
                        from_date= end_date

                        str_date=start_date.strftime("%Y-%m-%d")
                        print(f"Sync data: {str_date}")
                        self.log_info(f"Sync data: {str_date}")

                        sql=f"""
                            SELECT {fields_str}
                            FROM "{pg_schema_name}"."{table_name}" 
                            WHERE ("{created_ts_field_name}">= '{start_date}' AND "{created_ts_field_name}"<'{end_date}')
                            OR ("{updated_ts_field_name}">='{start_date}' AND "{updated_ts_field_name}"<'{end_date}')
                        """
                        batch=pg.pg_get_df(sql)
                        batch = batch.rename(columns={'Id': '_id'})

                        print(f"Data Type: {batch.dtypes}")
                        self.log_info(f"Data Type: {batch.dtypes}")
                        
                        # Change column to datatime
                        for col in ["CreatedAt", "UpdatedAt", "CreatedBy", "UpdatedBy","PromoStartAt","PromoEndAt"]:
                            if col in batch.columns:
                                batch[[col]] = batch[[col]].astype(object).where(batch[[col]].notnull(), None)
                                # batch[col] = pd.to_datetime(batch[col], errors='coerce', utc=True)
                                # #batch[col] = batch[col].apply(lambda x: x if pd.notnull(x) else None)
                                # batch[col] = batch[col].apply(lambda x: x.to_pydatetime() if pd.notnull(x) else None)

                        # convert NaN to None
                        #batch = batch.where(pd.notnull(batch), None)

                        # Record count
                        batch_record_count = batch.shape[0]

                        # print(f"Record Sync: {batch_record_count}")
                        # self.log_info(f"Record Sync: {batch_record_count}")

                        if batch_record_count>0:
                            migration_data_count=mongo.migrate_data(pg_df=batch, mongo_db_name=mongo_schema_name, mongo_collection_name=mongo_table_name, selected_pg_columns=selected_columns_list, column_field_mapping=column_field_mapping)

                            if (migration_data_count!=batch_record_count):
                                print(f"batch_record_count:migration_data_count={batch_record_count}:{migration_data_count}")
                                self.log_error(f"batch_record_count:migration_data_count={batch_record_count}:{migration_data_count}")
                                raise Exception("Unable to insert records into MongoDB.")

                            record_count = record_count + migration_data_count

                        # Update timestamp of ETL table
                        etl_upd_status = pg.update_last_sync_time(pg_table_name=table_name, last_sync_time=start_date)

                        if (etl_upd_status is None):
                            raise Exception (f"Unable to update ETL table for {table_name} table.")
                    
                        print(f"Inserted: {record_count}/{total_records}")
                        self.log_info(f"Inserted: {record_count}/{total_records}")
                        
                    print(f"{table_name} is migrated sucessfully.")
                    self.log_info(f"{table_name} is migrated sucessfully.")

                except Exception as e:
                    print(f"Error: {e}")
                    self.log_error(f"Error: {e}")
        
            return True
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Error: {e}")
            return None
        
