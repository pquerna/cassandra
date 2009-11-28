#!/usr/local/bin/thrift --java --php --py
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# Interface definition for Cassandra Service
#

namespace java org.apache.cassandra.service
namespace cpp org.apache.cassandra
namespace csharp Apache.Cassandra
namespace py cassandra
namespace php cassandra
namespace perl Cassandra

# Thrift.rb has a bug where top-level modules that include modules 
# with the same name are not properly referenced, so we can't do
# Cassandra::Cassandra::Client.
namespace rb CassandraThrift


#
# constants
#

# for clients checking that server and it have same thrift definitions.
# no promises are made other than "if both are equal, you're good."
# in particular, don't try to parse numeric information out and assume
# that a "greater" version is a superset of a "smaller" one.
const string VERSION = "0.5-beta1"


#
# data structures
#

struct Column {
   1: required binary name,
   2: required binary value,
   3: required i64 timestamp,
}

struct SuperColumn {
   1: required binary name,
   2: required list<Column> columns,
}

struct ColumnOrSuperColumn {
    1: optional Column column,
    2: optional SuperColumn super_column,
}


#
# Exceptions
#

# a specific column was requested that does not exist
exception NotFoundException {
}

# invalid request (keyspace / CF does not exist, etc.)
exception InvalidRequestException {
    1: required string why
}

# not all the replicas required could be created / read
exception UnavailableException {
}

# RPC timeout was exceeded.  either a node failed mid-operation, or load was too high, or the requested op was too large.
exception TimedOutException {
}

# (note that internal server errors will raise a TApplicationException, courtesy of Thrift)


#
# service api
#

enum ConsistencyLevel {
    ZERO = 0,
    ONE = 1,
    QUORUM = 2,
    DCQUORUM = 3,
    DCQUORUMSYNC = 4,
    ALL = 5,
}

struct ColumnParent {
    3: required string column_family,
    4: optional binary super_column,
}

struct ColumnPath {
    3: required string column_family,
    4: optional binary super_column,
    5: optional binary column,
}

struct SliceRange {
    1: required binary start,
    2: required binary finish,
    3: required bool reversed=0,
    4: required i32 count=100,
}

struct SlicePredicate {
    1: optional list<binary> column_names,
    2: optional SliceRange   slice_range,
}

struct KeySlice {
    1: required string key,
    2: required list<ColumnOrSuperColumn> columns,
}


service Cassandra {
  # retrieval methods
  ColumnOrSuperColumn get(1:required string keyspace,
                          2:required string key,
                          3:required ColumnPath column_path,
                          4:required ConsistencyLevel consistency_level=1)
                      throws (1:InvalidRequestException ire, 2:NotFoundException nfe, 3:UnavailableException ue, 4:TimedOutException te),

  list<ColumnOrSuperColumn> get_slice(1:required string keyspace, 
                                      2:required string key, 
                                      3:required ColumnParent column_parent, 
                                      4:required SlicePredicate predicate, 
                                      5:required ConsistencyLevel consistency_level=1)
                            throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  map<string,ColumnOrSuperColumn> multiget(1:required string keyspace, 
                                           2:required list<string> keys, 
                                           3:required ColumnPath column_path, 
                                           4:required ConsistencyLevel consistency_level=1)
                                  throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  map<string,list<ColumnOrSuperColumn>> multiget_slice(1:required string keyspace, 
                                                       2:required list<string> keys, 
                                                       3:required ColumnParent column_parent, 
                                                       4:required SlicePredicate predicate, 
                                                       5:required ConsistencyLevel consistency_level=1)
                                        throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  i32 get_count(1:required string keyspace, 
                2:required string key, 
                3:required ColumnParent column_parent, 
                4:required ConsistencyLevel consistency_level=1)
      throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  /** @deprecated; use get_range_slice instead */
  list<string> get_key_range(1:required string keyspace, 
                             2:required string column_family, 
                             3:required string start="", 
                             4:required string finish="", 
                             5:required i32 count=100,
                             6:required ConsistencyLevel consistency_level=1)
               throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  list<KeySlice> get_range_slice(1:required string keyspace, 
                                 2:required ColumnParent column_parent, 
                                 3:required SlicePredicate predicate,
                                 4:required string start_key="", 
                                 5:required string finish_key="", 
                                 6:required i32 row_count=100, 
                                 7:required ConsistencyLevel consistency_level=1)
                 throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  # modification methods
  void insert(1:required string keyspace, 
              2:required string key, 
              3:required ColumnPath column_path, 
              4:required binary value, 
              5:required i64 timestamp, 
              6:required ConsistencyLevel consistency_level=0)
       throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  void batch_insert(1:required string keyspace, 
                    2:required string key, 
                    3:required map<string, list<ColumnOrSuperColumn>> cfmap, 
                    4:required ConsistencyLevel consistency_level=0)
       throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  void remove(1:required string keyspace,
              2:required string key, 
              3:required ColumnPath column_path,
              4:required i64 timestamp,
              5:ConsistencyLevel consistency_level=0)
       throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),


  // Meta-APIs -- APIs to get information about the node or cluster,
  // rather than user data.  The nodeprobe program provides usage examples.

  // get property whose value is of type "string"
  string get_string_property(1:required string property),

  // get property whose value is list of "strings"
  list<string> get_string_list_property(1:required string property),

  // describe specified keyspace
  map<string, map<string, string>> describe_keyspace(1:required string keyspace)
                                   throws (1:NotFoundException nfe),

  // Set the timeout or all operations on this connection. 0 will use the server default.
  void setopt_timeout(1:i32 milliseconds),
}

