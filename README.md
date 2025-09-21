# NEXT (ROCKSDB + LSM-based Secondary Index) 

![Alt text](/FrontPage.png)

This is a prototype implementation of NEXT for RocksDB. NEXT is a new LSM-based secondary index framework that utilizes a two-level structure to improve non-key attribute query performance. 
More information about NEXT can be found in the paper "NEXT: A New Secondary Index Framework for LSM-based Data Storage" which is accepted by SIGMOD 2025. The Figure below shows an overview of NEXT which consists of per-segment components in SST files and a global index component resides in RAM.

![Alt text](/next_overview.png)

## Data and Queries Example

Examples of data (one millions tuples) and queries can be downloaded from 'https://drive.google.com/drive/folders/1GLobdy70c_DEpHJpVkLpo1nAmIly3tTL?usp=sharing'.

## Installation & Compile

To install RocksDB: Visit https://github.com/facebook/rocksdb/blob/main/INSTALL.md .

To compile static library
```
cd rockdb-7.7.3
make clean
make static_lib
```

## Run

To specify the location of global index component:

Updating the file 'rocksdb-7.7.3/db/version_set.h' @ line 1511:
```
const char* global_rtree_loc_ = "<Global Index Component Directory>";
```

To run static spatial database writing
1) update the global index component to be 2D R-tree ('rocksdb-7.7.3/db/version_set.h' @ 1007 & 1509; 'rocksdb-7.7.3/db/version_builder.h' @ 37):
```
typedef RTree<GlobalSecIndexValue, double, 2, double> GlobalSecRtree;
```
2) compile the script and run the writing
```
cd examples
make secondary_index_write
./secondary_index_write <Directory of the DB> <DB Size> <DB Source File>
```
Example:
```
./secondary_index_write home/UserName/NEXT/db_loc/db_name 1000000 /home/UserName/NEXT/Data_and_Queries/buildings_1m
```

To run static spatial database reading
```
cd examples
make secondary_index_read
./secondary_index_read <Directory of the DB> <Query Size> <Query File>
```
Example:
```
./secondary_index_read home/UserName/NEXT/db_loc/db_name 1000 /home/UserName/NEXT/Data_and_Queries/buildings_query_0.01
```



To run static numerical database writing
1) update the global index component to be 1D R-tree (i.e., Interval tree) ('rocksdb-7.7.3/db/version_set.h' @ 1007 & 1509; 'rocksdb-7.7.3/db/version_builder.h' @ 37):
```
typedef RTree<GlobalSecIndexValue, double, 1, double> GlobalSecRtree;
```
2) compile the script and run the writing
```
cd examples
make secondary_index_write_num
./secondary_index_write_num <Directory of the DB> <DB Size> <DB Source File>
```
Example:
```
./secondary_index_write_num home/UserName/NEXT/db_loc/db_name 1000000 /home/UserName/NEXT/Data_and_Queries/buildings_1m
```

To run static numerical database reading
```
cd examples
make secondary_index_read_num
./secondary_index_read_num <Directory of the DB> <Query Size> <Query File>
```
Example:
```
./secondary_index_read_num home/UserName/NEXT/db_loc/db_name 1000 /home/UserName/NEXT/Data_and_Queries/buildings_query_0.01
```