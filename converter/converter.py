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
        with client_hdfs.read(path, encoding='utf-8') as reader:
            df = pd.read_csv(reader, index_col=0)
        return df
    else:
        with open(path, 'r') as data:  # open csv file
            reader = csv.DictReader(data)  # create csv reader
            for row in reader:  # create csv file iterator
                yield row


def create_avro(path, name, client_hdfs):
    """Create avro file using schema
    Args:
        path: file path
        name: file name
        client_hdfs: hdfs client
    """
    schema = avro.schema.Parse(open(f"./{name[:-4]}_avro/{name[:-4]}_schema.avsc", "rb").read())  # read avro schema
    writer = DataFileWriter(open(f"./{name[:-4]}_avro/{name[:-4]}.avro", "wb"), DatumWriter(),
                            schema)  # create file and avro writer
    for row in read(path, client_hdfs):
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


def create_directory(name):
    """Create directory for new files
    Args:
        name: file name
    """
    os.mkdir(f'./{name[:-4]}_avro')


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


def convert(path, client_hdfs=None):
    """Convert csv file into avro
    Args:
        path: file path
        client_hdfs: hdfs client
    """
    regex = r"\w*\.\w*$"
    name = re.findall(regex, path)[0]
    create_schema(get_column(read(path, client_hdfs)), name)
    create_avro(path, name, client_hdfs)


def main():
    """Call convert function with CLI arguments"""
    parser = argparse.ArgumentParser(description='Command-line converter csv to avro')  # create CLI parser
    parser.add_argument("FILE", help="Csv file to convert")
    parser.add_argument("-hdfs", dest="HDFS_PATH", help="file location in hdfs")
    args = parser.parse_args()
    try:
        if args.FILE:
            convert(args.FILE)
        elif args.HDFS_PATH:
            client_hdfs = InsecureClient('http://' + os.environ['IP_HDFS'] + ':50070')
            convert(args.HDFS_PATH)
    except Exception as exception:
        print(exception)


if __name__ == '__main__':
    main()

#
# reader = DataFileReader(open(f"./{file_name}_avro/{file_name}.avro", "rb"), DatumReader())
# for user in reader:
#     print(user)
# reader.close()
