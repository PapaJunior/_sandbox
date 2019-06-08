import timeit
import time
import sqlalchemy
from sqlalchemy import MetaData, create_engine
metadata = MetaData()
engine = sqlalchemy.create_engine('postgresql://postgres:123456@localhost:5432/trassir')

# pos_incidents

from sqlalchemy import Table
pos_incidents = Table('pos_incidents', metadata, autoload=True, autoload_with=engine)

pos_incidents.columns.keys() # listing the columns
from sqlalchemy import select


def select_pos_incidents(limit=30):
    start_time = time.time()
    s = select([pos_incidents]).limit(limit) # selecting limit records
    res = engine.execute(s).fetchall()
    time_to_handle = time.time() - start_time
    print('Execution time: %s' % time_to_handle)
    print(len(res))
    return res

meta_data_of_pos_incidents_table = metadata.tables['pos_incidents']

#print(dir(metadata.tables['pos_incidents']))
#print(metadata.tables['pos_incidents'].columns)

"""
'add_is_dependent_on', 'alias', 'append_column', 'append_constraint', 'append_ddl_listener',
'argument_for', 'bind', 'c', 'columns', 'comment', 'compare', 'compile', 'constraints',
'correspond_on_equivalents', 'corresponding_column', 'count', 'create', 'delete', 'description',
'dialect_kwargs', 'dialect_options', 'dispatch', 'drop', 'exists', 'foreign_key_constraints', 
'foreign_keys', 'fullname', 'get_children', 'implicit_returning', 'indexes', 'info', 'insert',
'is_clause_element', 'is_derived_from', 'is_selectable', 'join', 'key', 'kwargs', 'lateral', 
'metadata', 'name', 'named_with_column', 'outerjoin', 'params', 'primary_key', 'quote', 'quote_schema',
'replace_selectable', 'schema', 'select', 'selectable', 'self_group', 'supports_execution',
'tablesample', 'tometadata', 'unique_params', 'update'
"""

event_log = Table('event_log', metadata, autoload=True, autoload_with=engine)
event_log.columns.keys()
meta_data_of_event_log = metadata.tables['event_log']


print(meta_data_of_pos_incidents_table.metadata.tables)
"""
'_remove_table', '_schema_item_copy', '_schemas', '_sequences',
 '_set_parent', '_set_parent_with_dispatch', '_translate_schema',
  'append_ddl_listener', 'bind', 'clear', 'create_all', 'dispatch', 
  'drop_all', 'get_children', 'info', 'is_bound', 'naming_convention',
   'quote', 'reflect', 'remove', 'schema', 'sorted_tables', 'tables']
"""
