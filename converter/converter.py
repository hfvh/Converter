#!/bin/bash
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import csv
import os
import json
import argparse


def create_avro(file):
    """Create avro file using schema
    Args:
        file: file name with format
    """
    name = file[:-4]  # correct file name without format to create folder
    schema = avro.schema.Parse(open(f"./{name}_avro/{name}_schema.avsc", "rb").read())  # read avro schema
    writer = DataFileWriter(open(f"./{name}_avro/{name}.avro", "wb"), DatumWriter(),
                            schema)  # create file and avro writer
    for row in read_funding_data(file):
        writer.append(row)  # write row to avro file
    writer.close()


def read_funding_data(path):
    """Create iterator for csv file
    Args:
        path: csv file path
    """
    with open(path, 'r') as data:  # open csv file
        reader = csv.DictReader(data)  # create csv reader
        for row in reader:  # create csv file iterator
            yield row


def create_schema(columns, name):
    """Create and store avro schema
    Args:
        columns: columns list
        name: file name
    """
    create_directory(name)
    pattern = {"type": "record", "namespase": "Tutorial", "name": f"{name}",
               "fields": []}  # simple avro pattern without columns names
    for column in columns:
        pattern["fields"].append({"name": column, "type": "string"})  # write column name in pattern
    with open(f"./{name}_avro/{name}_schema.avsc", "w+") as schema:  # create file for schema
        json.dump(pattern, schema)  # append pattern structure to json file


def create_directory(name):
    """Create directory for new files
    Args:
        name: file name
    """
    os.mkdir(f"./{name}_avro")


def get_column(data):
    """Find columns in csv format
    Args:
        data: csv file iterator
    Returns:
        The return columns list
    """
    columns = []
    for row in data:
        columns.extend(row.keys())  # get first row in csv file and find columns names
        break
    return columns


def convert(file):
    """Convert csv file into avro
    Args:
        file: file name with format
    """
    name = file[:-4]
    create_schema(get_column(read_funding_data(file)), name)
    create_avro(file)


def main():
    """Call convert function with CLI arguments"""
    parser = argparse.ArgumentParser(description='Command-line converter csv to avro')#create CLI parser
    parser.add_argument("File.csv", help="Csv file to convert")
    args = parser.parse_args()
    try:
        convert(args.File)
    except Exception as exception:
        print(exception)


if __name__ == '__main__':
    main()

# reader = DataFileReader(open(f"./{file_name}_avro/{file_name}.avro", "rb"), DatumReader())
# for user in reader:
#     print(user)
# reader.close()
