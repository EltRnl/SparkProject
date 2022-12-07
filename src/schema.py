import csv
from tabulate import tabulate
from sys import exit

## Private functions used by the class

def string_hash_or_integer(value):
    """Convert a given value to integer, and if not possible, to a string"""
    if value == '':
        return None
    try:
        return int(value)
    except:
        return str(value)

def parse_schema_line(row):
    """ Parse a row of the schema to transform the strings into desired format :        
        field number    -> <int> representing the index of the field
        content         -> <string> description of the field
        format          -> <string> representing the type of the data in field
        formatter       -> <func> function to turn the string in the field to the desired type (in format)
        mandatory       -> <bool> shows if the field always has a value (True) or not (False)
     """
    field_keys = ['field number', 'content', 'format', 'mandatory']
    filtered_row = {key: row[key] for key in field_keys if key in row}
    filtered_row['field number'] = int(filtered_row['field number'])-1
    formatters = {
        'STRING_HASH': lambda s: s if s != '' else None,
        'INTEGER': lambda i: int(i) if i != '' else None,
        'FLOAT': lambda f: float(f) if f != '' else None,
        'BOOLEAN': lambda b: bool(int(b)) if b != '' else None,
        'STRING_HASH_OR_INTEGER': string_hash_or_integer
    }
    filtered_row['formatter'] = formatters[filtered_row['format']]
    filtered_row['mandatory'] = True if filtered_row['mandatory'] == 'YES' else False
    return filtered_row

def make_field_getter(index):
    def getter(a_list):
            return a_list[index]
    return getter

## Schema class
class Schema:
    path = ""
    dictionary = dict()

    def __init__(self,path_to_schema: str, path_to_data: str = None):
        """
        Create a Schema object given the path to the schema file.

        The object will also determine the path to the data by assuming that the schema file is 
        in the folder containing all table folders. If it is not the case, please provide a path 
        to this data folder as second argument.
        """
        # Extracting the path to data
        if path_to_data is None:
            split_path = path_to_schema.split('/')[:len(path_to_schema.split('/'))-1]
            self.path = ("{}/"*len(split_path)).format(*split_path)
        else:
            self.path = path_to_data
            if self.path[-1] != '/':
                self.path += "/"
        
        # Loading schema file
        try:
            with open(path_to_schema, newline='') as schema_file:
                schema_reader = csv.DictReader(schema_file, delimiter=",")
                schema_rows = list(schema_reader)

                for row in iter(schema_rows):
                    source_name = row['file pattern'].split('/')[0]
                    
                    if source_name not in self.dictionary:
                        self.dictionary[source_name] = {
                            'file pattern': row['file pattern'],
                            'fields': list()
                        }
        except:
            print(f"ERROR: could not open file '{path_to_schema}', did you write the path well ?")
            exit()

        for row in iter(schema_rows):
            source_name = row['file pattern'].split('/')[0]
            self.dictionary[source_name]['fields'].append(parse_schema_line(row))

        for data_source in self.dictionary.values():
            data_source['fields'].sort(key=lambda field: field['field number'])
    
    def __str__(self) -> str:
        result: str = ""
        for key,value in self.dictionary.items():
            result += "#########################################################################"
            result += f"{key} ({value['file pattern']}):"
            result += tabulate(value['fields'],headers='keys')
        return result
    
    def print_fields(self):
        print("Fields present in schema :")
        for key,_ in self.dictionary.items():
            print("- "+key)

    def get_table_schema(self,table_name):
        if table_name in self.dictionary:
            return self.dictionary[table_name]
        else:
            print(f"ERROR: {table_name} is not a table of this schema.")
            self.print_fields()
            return None

    def load_rdd(self,spark_context,table_name):
        if table_name not in self.dictionary:
            print(f"ERROR: No table {table_name} found in schema.")
            self.print_fields()
            return None
        
        path_to_table = self.path + table_name + "/"
        rdd = None
        try:
            rdd_table = self.get_table_schema(table_name)
            rdd = spark_context.textFile(path_to_table).map(lambda row: [rdd_table['fields'][index]['formatter'](item) for index, item in enumerate(row.split(','))])
        except:
            print(f"ERROR: Something went wrong while opening and parsing '{path_to_table}'.")
        return rdd
    
    def index_of_field(self,table_name,field_name):
        for field in self.get_table_schema(table_name)['fields']:
            if field_name == field['content']:
                return field['field number']
        return None
    
    def field_getters(self,table_name):
        """
        Given the name of an rdd table, build a dictionary of getters.

        The getters are indexed by the column/field name (called "content" in the schema). They are functions which take a row of data (a list) and returns the value at the desired column/field. An example of usage:

        machine_events_getter = field_indexes(schemas['machine events'])
        row = [0, 1, 2, 3, 4, 5] # a placeholder replacing the real data
        machine_events_getter['event type'](row)
        """
        mapping = {}
        for field in self.get_table_schema(table_name)['fields']:
            mapping[field['content']] = make_field_getter(field['field number'])
        return mapping


if __name__ == '__main__':
    from pyspark import SparkContext

    sc = SparkContext("local[*]")
    sc.setLogLevel("ERROR")

    print("Testing Schema Class")
    print("Loading ../data/schema.csv")
    schema = Schema("../data/schema.csv")
    schema.print_fields()

    field = "machine_attributes"
    print(f'loading table : {field}')
    rdd = schema.load_rdd(sc,field)
    print(tabulate(rdd.take(5)))