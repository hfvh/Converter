#!/bin/bash
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import csv
import os
import json
import argparse
from hdfs import InsecureClient
import pandas as pd
import re


def read(path, client_hdfs=None):
    """File reader
    Args:
        path: file path
        client_hdfs: hdfs client
    """
    if client_hdfs:
        print("Client is hdfs")
        with client_hdfs.read(path, encoding='utf-8') as reader:
            for line in reader:
                yield line

    else:
        with open(path, 'r') as data:  # open csv file
            reader = csv.DictReader(data)  # create csv reader
            for row in reader:  # create csv file iterator
                yield row


def create_avro(path, name, data, client_hdfs):
    """Create avro file using schema
    Args:
        path: file path
        name: file name
        client_hdfs: hdfs client
    """
    schema = avro.schema.Parse(open(f"./{name[:-4]}_avro/{name[:-4]}_schema.avsc", "rb").read())  # read avro schema
    writer = DataFileWriter(open(f"./{name[:-4]}_avro/{name[:-4]}.avro", "wb"), DatumWriter(),
                            schema)  # create file and avro writer
    for row in data:
        writer.append(row)  # write row to avro file
    writer.close()
    if client_hdfs:
        new_path = path.split(name)[0]
        client_hdfs.write(new_path, "./{name[:-4]}_avro")


def create_schema(columns, name):
    """Create and store avro schema
    Args:
        columns: columns list
        name: file name
    """
    create_directory(name)
    pattern = {"type": "record", "namespase": "Tutorial", "name": f"{name[:-4]}",
               "fields": []}  # simple avro pattern without columns names
    for column in columns:
        pattern["fields"].append({"name": column, "type": "string"})  # write column name in pattern
    with open(f"./{name[:-4]}_avro/{name[:-4]}_schema.avsc", "w+") as schema:  # create file for schema
        json.dump(pattern, schema)  # append pattern structure to json file
    print("Schema created")


def create_directory(name):
    """Create directory for new files
    Args:
        name: file name
    """
    os.mkdir(f'./{name[:-4]}_avro')
    print("Folder created")


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


def get_column2(data):
    columns = []
    for row in data:
        columns = row.split(',')  # get first row in csv file and find columns names
        break
    return columns


def convert(path, client_hdfs=None):
    """Convert csv file into avro
    Args:
        path: file path
        client_hdfs: hdfs client
    """
    regex = r"\w*\.\w*$"
    data = read(path, client_hdfs)
    name = re.findall(regex, path)[0]
    if client_hdfs:
        create_schema(get_column2(data), name)
    else:
        create_schema(get_column(data), name)
    create_avro(path, name, data, client_hdfs)


def main():
    """Call convert function with CLI arguments"""
    parser = argparse.ArgumentParser(description='Command-line converter csv to avro')  # create CLI parser
    parser.add_argument("-file", dest="FILE", help="Csv file to convert")
    parser.add_argument("-hdfs", dest="HDFS", help="File location in hdfs")
    args = parser.parse_args()
    try:
        if args.FILE:
            convert(args.FILE)
        elif args.HDFS:
            client_hdfs = InsecureClient('http://sandbox-hdp.hortonworks.com' + ':50070')
            print("Connection with hdfs...")
            convert(args.HDFS, client_hdfs)
    except Exception as exception:
        print(exception)


if __name__ == '__main__':
    main()

#
# reader = DataFileReader(open(f"./{file_name}_avro/{file_name}.avro", "rb"), DatumReader())
# for user in reader:
#     print(user)
# reader.close()
