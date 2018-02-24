"""
The `crud` module defines a number of functions for finding SQLAlchemy model
objects via a query parameter and displaying a desired portion of the resulting
object graph via a data specification parameter, optionally limiting the total
number returned, potentially with an offset to support paging.


QUERY PARAMETER
---------------
The format of the query parameter needs to support logical operators and a
certain amount of introspection into which model objects are involved in a
give query. For this writeup, a "query" is any set of search parameters that
will result in a known SQL search string capable of returning the desired
model objects. Python syntax will be used to represent the expected format of
the method parameters, with allowances for representing infinite nesting/lists
as appropriate. Unless explicitly stated, pluralized forms like "queries" can
be read as "query or queries" due to the support of one or more queries in all
cases.

The comprehensive form of the query parameter is as follows::

    query = [{
        '_model': <model_name>,
        '_data': <data_specification>,
        '_label': <query_label>,
        # Either provide <logical_operator> OR items after <logical_operator>
        <logical_operator>: [<query>[, <query>]*],
        # Used IF AND ONLY IF <logical_operator> is not provided
        'comparison': <comparison_function>
        'field': <model_field_name>,
        'value': <model_field_value>
    }]

meaning an array of one or more dictionaries (a dictionary is equivalent to an
array of length 1) of queries, one for each type of SQLAlchemy model object
expected to be returned.

where:
- '<model_name>' - the string corresponding to the SQLAlchemy model class name
    which extends your @residue.declarative_base
- '<query_label>' - the optional string that signifies the purpose of this
    query and is only used as a convenience for the consumer of the crud
    method. This primarily supports counts, but can used in client code to
    help cue the display of those results, defaults to the contents of _model
- '<logical_operator>' - the key is one of the following logical operators
    (with the value being one of more queries in a list):
-- and ("intersection")
-- or ("union")
--- meaning that the results of the provided queries will be the corresponding
    intersection/union of all the results of an individual query. Imagining a
    Venn Diagram is useful in this instance.
- <query> - is a dictionary identical the dictionary taken in by the query
    parameter EXCEPT that _model is not included
- <comparison> - a comparison operator function used to find the objects that
    would return "True" for the provided comparison for the value in the
    model_field_name. Some examples are:
-- 'lt' - is the field less than value?
-- 'gt' - is the field greater than value?
-- 'eq' - is the field equal to value? (default)
-- 'ne' - is the field not equal value?
-- 'le' - is the field less than or equal to value?
-- 'ge' - is the field greater than or equal to value?
-- 'isnull' - does the field have a null value? (does not use the query's
    value parameter)
-- 'isnotnull' - does the field have a non null value? (does not use the
    query's value parameter)
-- 'in' - does the field appear in the given value? (value should be an array)
-- 'contains' - does the field contain this value? (would allow other
    characters before or after the value)
-- 'like' - same as contains, but case sensitive
-- 'ilike' - same as contains, case insensitive
-- 'startswith' - does the field start with value?
-- 'istartswith' - case insensitive startswith
-- 'endswith' - does the field end with value?
-- 'iendswith' - case insensitive endswith

- <model_field_name> - the name of the field for the provided _model at the
    top level. Supports dot notation, e.g.:
-- making a comparison based off all of a Team's players' names would use an
    'field' of 'player.name'
- <model_field_value> - the value that the field comparison will be made
    against, e.g. a value of 'text' and a comparison of 'eq' will return all
    matching models with fields equal to 'text'.
- <data_specification> - specifying what parts of the results get returned,
    the following section covers the format the data specification parameter


DATA SPECIFICATION
------------------
Where the query parameter is only used to search existing objects in the
database, the data specification parameter has two separate meanings: in the
'read' function as the _data key in the query dictionary: what information is
returned in the results, in the 'update' and 'create' functions, what model
type will be created/updated with what values. This is encompassed in one
format, so there is some amount of redundancy depending on what actions you're
performing.

The comprehensive form of the data specification parameter is as follows:

data = [{
    '_model': <model_name>,
    # a non-foreign key field
    '<model_field_name>': True (or the value of the field if the data
        parameter is used to create or update objects)
    # a foreign key field is a special case and additional forms are supported)
    '<foreign_key_model_field_name>': True (all readable fields of the
        reference model object will be read. Has no meaning if the data
        parameter is used to create of update objects)
    '<foreign_key_model_field_name>': {<same form as the data parameter, e.g.,
        supports recursion}
}] +

Meaning an array of one or more dictionaries (a dictionary is equivalent to an
array of length 1) of data specification, one for each type of LDAP model
object expected to be returned. As a special case for the 'read' method, one
dictionary is interpreted as being the intended data spec for each item in the
query parameter array. In the 'update' array, the length of the query and data
parameters must match and the nth member of both the query and data array are
read together as a matched set. In the 'create' method, each member of the
data array will create a new object of the type specified in _model.

A supported short form of a data specification is, instead of a dictionary of
key names with values, a list of key names that should be read:

['<model_field_name_1',
 '<model_field_name_2',
 '<model_field_name_3']

is equivalent to:

{'<model_field_name_1': True,
 '<model_field_name_2': True,
 '<model_field_name_3': True}

As you can see, that this short form would not be appropriate for create or
updates function calls, as there's no way to specify the desired values.
Additionally there's no way to specify a sub-object graph for a followed
foreign key.


RESULTS FOR crud.count
----------------------
The crud.count method accepts a query parameter (format examined above) and
returns a count of each of the supplied queries (typically, this is a count of
each supplied model type), however the results also include a _label key, that
can be used to differentiate between two different types of results within the
same model type (e.g. enabled accounts vs disabled accounts)

e.g.:

return [{
    _model : 'Team',
    _label: 'Team',
    count : 12
}, {
    _model : 'Player',
    _label : 'Players on a Team',
    count : 144
}, {
    _model : 'Manager',
    _label : 'Managers of a Team',
    count : 12
}, {
    _model : 'LeagueEmployee',
    _label : 'Everyone employed by the league (e.g. Players, Managers)',
    count : 156
}]


RESULTS FOR crud.read
---------------------
The crud.read method accepts both a query and data specification parameter
(format examined above), and two parameters for fine-tuning which specific
results are returned (examined in the upcoming "Fine-Tuning Read Results"
section. The read method returns the total number of objects matching the
query (separate from any sort of limits) and a list of the specific objects
requested (subject to those limits) e.g.

return {
    total: 20 # count of ALL matching objects
    # although only 5 results were returned as a result of the
    # specified fine-tuning parameters
    results: [<result>, <result>, <result>, <result>, <result>]
}

To prevent the client from always being forced to deal with entire query
result, there are three parameters in place for the crud.read method to
simplify only receiving the information that's desired. At a high level:

- 'limit' takes a positive integer 'L' and when provided, the crud.read
    method will return at most L results, defaults to no limit
- 'order' takes a list of order specification dictionaries for sorting by
    specific fields and in a specified direction (ascending or descending),
    defaults to no ordering
- 'offset' takes a positive integer 'F' and when provided, the crud.read
    method will return at most L results, after skipping the first (based
    on the order specification) F results.

Used with the crud.read method to only return only a subset of information,
allowing the client to only receive the amount of information it's interested
in. Useful in conjunction with the offset and ordering parameter to
finely-tune the information received.

The comprehensive form of the order parameter is as follows:

order = [{
    'dir': <'asc'/'desc'> # either in ascending (default) or descending order
    'fields': [['<model_object_name>.]<model_field_name>']+
}] +

A single string in 'fields' is equivalent to a list with the string as the
only element. If no model_object name is provided, the model_field_name is
interpreted as the catch-all key for all model objects. If model_field_name
isn't present on a model, or no catch-all is specified, 'id' will be used

The list of dictionaries are interpreted as being ordered in decreasing
priority. An example:

The 'offset' parameter is used with the crud.read method to only return only
a subset of information, allowing the client to only receive the amount of
information it's interested in. Useful in conjunction with the limit and
ordering parameter to finely-tune the information received.

Using the 4 records in the order example (including the order specification):
- a limit of 1 with an offset of 0 (the default if unspecified) would return
    only the John Depp Human.
- a limit of 0 (unlimited, which is the default if unspecified) and an offset
    of 0 would be identical to the table in the order-only example
- a limit of 0 and an offset of 1 would return everything except for the first
    result, so in this case, the last 3 results
- a limit of 2 and an offset of 1 would return the 2nd and 3rd results, so in
    this case, the middle 2 results

"""


from residue.crud.api import *  # noqa: F401,F403
from residue.crud.orm import *  # noqa: F401,F403
