from explain import DB, Graph
from pprint import pp

host = "aws-0-ap-southeast-1.pooler.supabase.com"
port = "5432"
database = "postgres" 
user = "postgres.jvbpkioaofgmhwkwdsvg"
password = "souravkontol" 


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

g = Graph(db.get_query_plan())
