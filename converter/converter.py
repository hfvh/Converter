#!/bin/bash
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import csv
import os
import json
import argparse
from hdfs import InsecureClient
import shutil


def read(path, name, client_hdfs=None):
    """File reader
    Args:
        path: file path
        client_hdfs: hdfs client
    """
    file = []
    client_hdfs.download(path, local_path='./')
    with open(name, 'r') as data:  # open csv file
        reader = csv.DictReader(data)  # create csv reader
        for row in reader:  # create csv file iterator
            file.append(row)
    return file


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
    rm_processing_files(path, name, client_hdfs)


def rm_processing_files(path, name, client_hdfs):
    new_path = path.split(name)[0]
    client_hdfs.upload(new_path + "avro", f"./{name[:-4]}_avro/")
    shutil.rmtree(f"./{name[:-4]}_avro")
    os.remove(f"./{name}")


def create_schema(columns, name):
    """Create and store avro schema
    Args:
        columns: columns list
        name: file name
    """
    pattern = {"type": "record", "namespase": "Tutorial", "name": f"{name[:-4]}",
               "fields": []}  # simple avro pattern without columns names
    for column in columns:
        pattern["fields"].append({"name": column, "type": "string"})  # write column name in pattern
    return pattern


def get_column(data):
    """Find columns in csv format
    Args:
        data: csv data list
    Returns:
        The return columns list
    """
    return list(data[0].keys())


def convert(path, client_hdfs=None):
    """Convert csv file into avro
    Args:
        path: file path
        client_hdfs: hdfs client
    """
    name = os.path.basename(path)
    data = read(path, name, client_hdfs)
    os.mkdir(f'./{name[:-4]}_avro')
    print("Folder created")
    pattern = create_schema(get_column(data), name)
    with open(f"./{name[:-4]}_avro/{name[:-4]}_schema.avsc", "w+") as schema:  # create file for schema
        json.dump(pattern, schema)  # append pattern structure to json file
    print("Schema created")
    create_avro(path, name, data, client_hdfs)


def csv_reader(path, client_hdfs):
    """Read first 10 avro row
    Args:
        path: file path
        client_hdfs: hdfs client
    """
    client_hdfs.download(path, local_path='./')
    name = os.path.basename(path)
    reader = DataFileReader(open(f"./{name}", "rb"), DatumReader())
    point = 0
    for user in reader:
        print(user)
        point += 1
        if point == 10:
            break
    reader.close()
    os.remove(f"./{name}")


def main():
    """Call convert function with CLI arguments"""
    parser = argparse.ArgumentParser(description='Command-line converter csv to avro')  # create CLI parser
    parser.add_argument("-read", dest="FILE", help="Csv file to read")
    parser.add_argument("-hdfs", dest="HDFS", help="File location in hdfs")
    args = parser.parse_args()
    try:
        client_hdfs = InsecureClient('http://sandbox-hdp.hortonworks.com' + ':50070')
        print("Connection with hdfs...")
        if args.FILE:
            csv_reader(args.FILE, client_hdfs)
        elif args.HDFS:
            convert(args.HDFS, client_hdfs)
    except Exception as exception:
        print(exception)


if __name__ == '__main__':
    main()


