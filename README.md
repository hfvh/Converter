Converter
---------

Command-line converter csv to avro for hdfs.

Getting Started
---------------

1. Create avro file and put it in hdfs:
``` markdown
converter -hdfs </user/sample/file.csv>
``` 
2. Read first 10 avro file rows from hdfs:
```markdown
converter -read </user/sample/avro/file.avro>
```

Installing
-------------
``` markdown
$ git clone https://github.com/hfvh/Converter.git
$ cd Converter/
$ sh install_python36.sh
$ sh install_converter.sh
```

Documentation
-------------
