import sys
from pyspark import SparkContext
import time
from timeit import default_timer as timer


def string_hash_or_integer(value):
    """Convert a given value to integer, and if not possible, to a string"""
    try:
        return int(value)
    except:
        return str(value)

def parse_schema_line(row):
    """ Parse a row of the schema to transform the strings into desired format :        
        field number    -> int representing the index of the field
        content         -> string description
        format          ->
        mandatory       ->
     """
    field_keys = ['field number', 'content', 'format', 'mandatory']
    filtered_row = {key: row[key] for key in field_keys if key in row}
    filtered_row['field number'] = int(filtered_row['field number'])-1
    formatters = {
        'STRING_HASH': lambda s: s,
        'INTEGER': lambda i: int(i),
        'FLOAT': lambda f: float(f),
        'BOOLEAN': lambda b: bool(int(b)),
        'STRING_HASH_OR_INTEGER': string_hash_or_integer
    }
    filtered_row['formatter'] = formatters[filtered_row['format']]
    filtered_row['mandatory'] = True if filtered_row['mandatory'] == 'YES' else False
    return filtered_row


# # Parse the schema file
def data_schemas_from_file(filename):
    """ Parse the schema.csv file which describes the fields of any kind of data represented in the data set.
    
        This piece of code is based on the assumption that the fields described in schema.csv appear in ascending order, from first to last.

        Returns a dictionary indexed on the data type (first part of the file pattern), which contains the file pattern and a list of fields.
        Each field is itself a dictionary with the entries 'field number', 'content', 'format', 'formatter, and 'mandatory'. 'formatter' is a function we add, to be used to format the input data into an appropriate python type.
    """
    import csv
    schemas = dict()
    
    with open(filename, newline='') as schema_file:
        schema_reader = csv.DictReader(schema_file, delimiter=",")
        schema_rows = list(schema_reader)

        for row in iter(schema_rows):
            source_name = row['file pattern'].split('/')[0]
            
            if source_name not in schemas:
                schemas[source_name] = {
                    'file pattern': row['file pattern'],
                    'fields': list()
                }

        
        for row in iter(schema_rows):
            source_name = row['file pattern'].split('/')[0]
            schemas[source_name]['fields'].append(parse_schema_line(row))

        return schemas

if __name__ == '__main__':
    schemas = data_schemas_from_file("../data/schema.csv")

    from tabulate import tabulate

    for key,value in schemas.items():
        print("#########################################################################")
        print(f"{key} ({value['file pattern']}):")
        print(tabulate(value['fields'],headers='keys'))