from explain import DB, Graph, GraphVisualizer
from pprint import pp

host = ""
port = ""
database = "" 
user = ""
password = "" 


db = DB({
    "host": host, 
    "port": port, 
    "database": database, 
    "user": user, 
    "password": password
})

pp(db.get_query_plan("SELECT * FROM customer c JOIN orders o ON o.o_custkey = c.c_custkey LIMIT 100;"))
pp(db.get_query_plan_analysis("SELECT * FROM customer c JOIN orders o ON o.o_custkey = c.c_custkey LIMIT 100;"))
pp(db.get_table_names())
pp(db.get_distinct_row_count('customer', 'c_nationkey'))
pp(db.get_column_names('customer'))
pp(db.statistics)
graph = Graph(db.get_query_plan("SELECT * FROM customer c JOIN orders o ON o.o_custkey = c.c_custkey LIMIT 100;"))
graphviz = GraphVisualizer(graph)
