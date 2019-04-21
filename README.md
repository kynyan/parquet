# Parquet

## Introduction

Parquet is a solution for parallel data writing to column-oriented file format. 

### Design ideas

- producer-consumer pattern based on BlockingQueue: 1 producer generates random data and puts it to the queue, records
are accumulated until they reach number specified in config. Consumers take data from the queue and write data 
to parquet file. Number of consumers is configurable (default is 4).

- data are split into datasets according to the configured size and written to partition files. Each partition file
contains timestamp, field (column) name, data type and values 

- each dataset contains metadata file which stores number of partitions, field names, data types and timestamp

### How to use

- all configurable properties are in application.properties file

