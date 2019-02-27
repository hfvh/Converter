import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import csv
import os
import json
import argparse


def create_avro(file_name, file):
    schema = avro.schema.Parse(open(f"../{file_name}_avro/{file_name}_schema.avsc", "rb").read())
    writer = DataFileWriter(open(f"../{file_name}_avro/{file_name}.avro", "wb"), DatumWriter(), schema)
    for row in read_funding_data(file):
        writer.append(row)
    writer.close()


def read_funding_data(path):
    with open(path, 'r') as data:
        reader = csv.DictReader(data)
        for row in reader:
            yield row


def create_schema(colomns, file_name):
    create_directory(file_name)
    pattern = {"type": "record", "namespase": "Tutorial", "name": f"{file_name}", "fields": []}
    for colomn in colomns:
        pattern["fields"].append({"name": colomn, "type": "string"})
    with open(f"../{file_name}_avro/{file_name}_schema.avsc", "w+") as schema:
        json.dump(pattern, schema)


def create_directory(file_name):
    os.mkdir(f"../{file_name}_avro")


def get_colomn(data):
    colomns = []
    for row in data:
        colomns.extend(row.keys())
        break
    return colomns


def convert(file):
    file_name = file[:-4]
    create_schema(get_colomn(read_funding_data(file)), file_name)
    create_avro(file_name, file)


def main():
    """Calc main function"""
    parser = argparse.ArgumentParser(description='Command-line converter csv to avro')
    parser.add_argument("File", help="Csv file to convert")
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
