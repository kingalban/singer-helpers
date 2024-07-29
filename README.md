# singer helpers

Tools to help with development of singer tap and targets.


## select-streams

A cli tool for managing streams selected in a singer catalog.

Example usage:
~~~ bash
# list streams and metadata
$ select-streams catalog.json --list
| stream          |  selected  | selected-by-default  |  replication-method  |
+-----------------+------------+----------------------+----------------------+
| accounts        |    True    |         True         |         None         |
| users           |    True    |         True         |         None         |
| transactions    |    True    |         True         |         None         |
+-----------------+------------+----------------------+----------------------+

# select accounts stream, print to stdout
$ select-streams catalog.json -s accounts --mode print
# {
#   "streams": [
#     {
#       "tap_stream_id": "accounts",
#       ...
#       "metadata": [
#         {
#           "breadcrumb": [],
#           "metadata": {
#             "inclusion": "available",
#             "selected": true,
#             "table-key-properties": [
#               "id"
#             ]
#           }
#         }
#         ...
#       ]
#     }
#     ...
#   ]
# }

# select all except accounts, save to file and print file name
select-streams catalog.json -x accounts
# /tmp/temp_catalog__excluded-orders.json
~~~

Also, you can set the `replication-method` and `forced-replication-method`, or reduce the schemas to `{}`.


## validate-json

Validate json records against a schema using a specified validator revision.
