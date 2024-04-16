import psycopg2
import graphviz
import os
from pprint import pp
  
BLOCK_SIZE = 128 # need to be researched to actually know how much is this

class Node: 
    def __init__(self, query_plan): 
        self.node_type = query_plan['Node Type']
        self.startup_cost = query_plan['Startup Cost']
        self.total_cost = query_plan['Total Cost']
        self.row_count = query_plan['Plan Rows']
        self.output = query_plan['Output']
        self.children = [] 
        
    def get_block_count(self): 
        return self.count / BLOCK_SIZE

    def get_distinct_rows(self, table_name, column_name): 
        return 0 
    
class Graph:    
    def __init__(self, query_plan): 
        self.root = self.parse_query_plan(query_plan)

    def parse_query_plan(self, query_plan):
        node = Node(query_plan)
        if 'Plans' in query_plan: 
            for child_query_plan in query_plan['Plans']: 
                node.children.append(self.parse_query_plan(child_query_plan)) 

        return node 
    
class GraphVisualizer: 
    def __init__(self, graph):
        self.graphviz = graphviz.Digraph('G', filename='qep', format='png')
        self.graphviz.attr(rankdir='BT')
        self.parse_graph(graph.root)
        self.graphviz.render('qep')

    def parse_graph(self, node):
        if node.children: 
            for child in node.children: 
                self.graphviz.edge(child.node_type, node.node_type)
                self.parse_graph(child)

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
        query_plan = self.execute("EXPLAIN (FORMAT JSON, VERBOSE TRUE, BUFFERS TRUE, ANALYZE TRUE) " + query)[0][0][0][0]['Plan']
        return query_plan

    def get_cpu_tuple_cost(self):
        return float(self.execute("""
                show cpu_tuple_cost;
            """)[0][0][0])
    
    def get_cpu_seq_page_cost(self):
        return float(self.execute("""
                show cpu_seq_page_cost;
            """)[0][0][0])
    
    def get_random_page_costs(self):
        return float(self.execute("""
                show random_page_costs;
            """)[0][0][0])

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
