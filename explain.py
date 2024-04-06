import psycopg2
import graphviz
from pprint import pp
class Graph: 
    def __init__(self, query_plan): 
        self.g = graphviz.Digraph('G', filename='qep')
        self.g.attr(rankdir='BT')
        self.parse_query_plan(query_plan)
        self.g.view() 

    def parse_query_plan(self, query_plan):
        if 'Plans' in query_plan: 
            for child_query_plan in query_plan['Plans']: 
                self.g.edge(child_query_plan['Node Type'], query_plan['Node Type'])
                self.parse_query_plan(child_query_plan) 

class DB: 
    def __init__(self, config): 
        self.host = config['host']
        self.port = config['port']
        self.database = config['database']
        self.user = config['user']
        self.password = config['password']
        self.connection = psycopg2.connect(host=self.host, port=self.port, database=self.database, user=self.user, password=self.password)
        self.cursor = self.connection.cursor()
        self.statistics = {} # self.get_statistics()

    def get_query_plan(self, query: str): 
        query_plan = self.execute("EXPLAIN (FORMAT JSON) " + query)[0][0][0][0]['Plan']
        return query_plan

    def get_query_plan_analysis(self, query: str): 
        query_plan_analysis = self.execute("EXPLAIN ANALYZE " + query)
        return query_plan_analysis 

    def execute(self, query: str):
        self.cursor.execute(query)
        column_names = [description[0] for description in self.cursor.description]
        query_results = self.cursor.fetchall()
        return query_results, column_names

    def is_query_valid(self, query: str):    
        try:
            self.cursor.execute(query)
            self.cursor.fetchone()
        except Exception as exception:
            return False, exception
        
        return True, None

    def get_table_names(self): 
        query_results = self.execute(
            """
            SELECT 
                t.table_name 
            FROM 
                information_schema.tables t
            WHERE 
                t.table_schema = 'public' 
                AND t.table_type = 'BASE TABLE' 
            """
        )
        table_names = [] 
        for table_name, in query_results[0]: 
            table_names.append(table_name)
        return table_names

    def get_distinct_row_count(self, table_name, column_name): 
        query_results = self.execute(
            """
            SELECT 
                COUNT(DISTINCT({column_name})) 
                FROM {table_name}
            """.format(column_name=column_name, table_name=table_name)
        )

        return query_results[0][0][0]
    
    def get_row_count(self, table_name: str): 
        query_results = self.execute(
            """
            SELECT 
                COUNT(*) 
                FROM {table_name}
            """.format(table_name=table_name)
        )

        return query_results[0][0][0]

    def get_column_names(self, table_name: str): 
        query_results = self.execute(
            """
            SELECT t.column_name 
            FROM information_schema.columns t 
            WHERE t.table_name = '{table_name}'
            """.format(table_name=table_name)
        )
        column_names = [] 
        for column_name, in query_results[0]: 
            column_names.append(column_name)

        return column_names

    def get_statistics(self): 
        statistics = {} 
        for table_name in self.get_table_names(): 
            table_statistics = {} 
            table_statistics['row_count'] = self.get_row_count(table_name)
            distinct_row_count = {} 
            for column_name in self.get_column_names(table_name): 
                distinct_row_count[column_name] = self.get_distinct_row_count(table_name, column_name)
            
            table_statistics['distinct_row_count'] = distinct_row_count 
            statistics[table_name] = table_statistics 
        
        return statistics 
