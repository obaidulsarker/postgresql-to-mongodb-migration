import xml.etree.ElementTree as ET
from logger import *
from setting import get_variables
from task import *

class Index(Logger):
    def __init__(self, logfile, pg_schema_name, mongo_schema_name, table_no, table_name, fields_list, id_field_name, created_ts_field_name, updated_ts_field_name, status, mongo_table_name, selected_columns_list, column_field_mapping):
        super().__init__(logfile)
        self.pg_schema_name = pg_schema_name
        self.mongo_schema_name = mongo_schema_name
        self.table_no = table_no
        self.table_name = table_name
        self.id_field_name = id_field_name
        self.created_ts_field_name = created_ts_field_name
        self.updated_ts_field_name = updated_ts_field_name
        self.fields_list=fields_list
        self.status=status
        self.mongo_table_name=mongo_table_name
        self.selected_columns_list=selected_columns_list
        self.column_field_mapping=column_field_mapping

    def __str__(self):
        return f"Schema Name:{self.pg_schema_name}, Table No: {self.table_no}, Table Name: {self.table_name}, Id Field Name: {self.id_field_name}, Timestamp Field Name: {self.created_ts_field_name}, Fields: {self.fields_list},  status: {self.status}"
    
class XmlReader(Logger):
    def __init__(self, logfile):
        super().__init__(logfile)
        self.operation_log = logfile
        self.indexes_xml_file_path = get_variables().INDEXES_XML_FILE_PATH
        self.total_collection=0

    # Read collection from xml file and load into task LIST and return a list
    def get_collection_list(self):
        try:
            xml_file=self.indexes_xml_file_path

            # Parse the XML file
            tree = ET.parse(xml_file)
            root = tree.getroot()
            
            # Create a list to store Task objects
            table_list = []
           

            table_no=0
            # Iterate through the queries and execute them
            for collections_element in root.findall('schema'):
                pg_schema_name = collections_element.get('pg_schema_name')
                mongo_schema_name = collections_element.get('mongo_schema_name')

                print(f"schema_name={pg_schema_name}")

                for query in collections_element.findall('table'):
                    selected_columns = []
                    column_field_mapping = {}
                    
                    # Default essential mappings for incremental logic
                    column_field_mapping["Id"] = "_id"

                    #collection_no = query.get("collection_no")
                    table_no = table_no + 1
                    table_name = query.get("table_name")
                    mongo_table_name=query.get("mongo_table_name")
                    id_field_name = query.get("id_field_name")
                    created_ts_field_name = query.get("created_ts_field_name")
                    updated_ts_field_name = query.get("updated_ts_field_name")
                    status = query.get("status")
                    fields_list = query.find("fields").text.strip()

                    print(f"table_name={table_name}")

                    # Selected Fields
                    if fields_list is not None:
                        configured_columns = [col.strip() for col in fields_list.split(',') if col.strip()]
                        selected_columns.extend(configured_columns)

                    # Parse column mapping
                    mapping_elem = query.find('field_mapping')
                    if mapping_elem is not None:
                        for map_elem in mapping_elem.findall('map'):
                            pg_name = map_elem.get('pg_name')
                            mongo_name = map_elem.get('mongo_name')
                            print(f"mongo_name={mongo_name}")
                            if pg_name and mongo_name:
                                column_field_mapping[pg_name] = mongo_name

                    # Remove duplicates from selected_columns while preserving order
                    selected_columns = list(dict.fromkeys(selected_columns))

                    print(f"selected_columns: {selected_columns}")
                    self.log_info(f"selected_columns: {selected_columns}")

                    print(f"column_field_mapping: {column_field_mapping}")
                    self.log_info(f"column_field_mapping: {column_field_mapping}")

                    index_obj = Index(logfile=self.log_file, pg_schema_name=pg_schema_name, mongo_schema_name=mongo_schema_name, table_no=table_no, table_name=table_name,id_field_name=id_field_name,created_ts_field_name=created_ts_field_name, updated_ts_field_name=updated_ts_field_name, fields_list=fields_list, status=status, mongo_table_name=mongo_table_name, selected_columns_list=selected_columns, column_field_mapping=column_field_mapping)
                    table_list.extend([index_obj])
                    #print (task)
                    self.total_collection = self.total_collection +1
                
            return table_list
        
        except Exception as e:
            self.log_error(f"Exception: {str(e)}")
            print (f"Exception: {str(e)}")
            return None
    