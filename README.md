# Rain or Ride

## Project Overview

### Members

|     Name      | NetID  |
| :-----------: | :----: |
| Bongjun Jang  | bj2351 |
|  Luka Tragic  | lt2205 |
| Terrance Chen | tc3325 |

### Purpose

## Data Access in HDFS

### Location

You can access the data resides in HDFS, by issueing the following command in dataproc.

```
hdfs dfs -ls /user/bj2351_nyu_edu/final
```

### Permissions

The directory has following permissions:

```
hdfs dfs -getfacl /user/bj2351_nyu_edu/final
# file: /user/bj2351_nyu_edu/final
# owner: bj2351_nyu_edu
# group: bj2351_nyu_edu
user::rwx
user:lt2205_nyu_edu:rwx
user:tc3180_nyu_edu:rwx
group::r-x
mask::rwx
other::r-x
```
