import psycopg2
import graphviz
import random
from pprint import pp

epsilon = 0.1
class DB: 
    def __init__(self, config): 
        self.host = config['host']
        self.port = config['port']
        self.database = config['database']
        self.user = config['user']
        self.password = config['password']
        self.connection = psycopg2.connect(host=self.host, port=self.port, database=self.database, user=self.user, password=self.password)
        self.cursor = self.connection.cursor()
        self.statistics = self.get_statistics()

        """ 
        Execute Analyze command if for all tables, the analyze or autoanalyze have never been done for each table. 
        This can be done safely because the data is static for this project (There is no Upsert operation). 
        """
        self.cursor.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_stat_all_tables
                    WHERE last_analyze IS NOT NULL OR last_autoanalyze IS NOT NULL
                ) THEN
                    RAISE NOTICE 'Running ANALYZE on all tables.';
                    ANALYZE;
                ELSE
                    RAISE NOTICE 'ANALYZE has already been run on some tables.';
                END IF;
            END $$;
        """) 

    def reset_connection(self):
        self.connection = psycopg2.connect(host=self.host, port=self.port, database=self.database, user=self.user, password=self.password)
        self.cursor = self.connection.cursor()
        
    def get_query_plan(self, query: str): 
        query_plan = self.execute("EXPLAIN (FORMAT JSON, VERBOSE TRUE, BUFFERS TRUE, ANALYZE TRUE) " + query)[0][0][0][0]['Plan']
        return query_plan

    def get_cpu_tuple_cost(self):
        return float(self.execute("""
                show cpu_tuple_cost;
            """)[0][0][0])
    
    def get_seq_page_cost(self):
        return float(self.execute("""
                show seq_page_cost;
            """)[0][0][0])
    
    def get_random_page_cost(self):
        return float(self.execute("""
                show random_page_cost;
            """)[0][0][0])
    
    def get_cpu_operator_cost(self): 
        return float(self.execute("""
                show cpu_operator_cost;
            """)[0][0][0])

    def get_table_statistics(self, table_name): 
        query_results = self.execute("""
            SELECT * FROM pg_class WHERE relname = '{table_name}';
            """.format(table_name=table_name))

        table_statistics = {} 
        for column_value, column_name in zip(query_results[0][0], query_results[1]):
            table_statistics[column_name] = column_value
        return table_statistics
    
    def get_table_page_count(self, table_name): 
        return self.statistics[table_name]['relpages']
    
    def get_table_row_count(self, table_name): 
        return self.statistics[table_name]['reltuples']

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
            statistics[table_name] = self.get_table_statistics(table_name)
        
        return statistics 

class Node: 
    def __init__(self, query_plan, db: DB): 
        self.db = db 
        self.uuid = str(random.random())
        self.node_type = query_plan['Node Type']
        self.startup_cost = query_plan['Startup Cost']
        self.total_cost = query_plan['Total Cost']
        self.row_count = query_plan['Plan Rows']
        self.output = query_plan['Output']
        self.filter = query_plan['Filter'] if 'Filter' in query_plan else ""
        self.relation_name = query_plan['Relation Name'] if 'Relation Name' in query_plan else ""
        self.children = [] 
        self.cost_description = self.get_cost_description() 
        
    def get_cost_description(self): 
        if self.node_type == 'Seq Scan':
            if self.filter: 
                return self.get_cost_description_sequential_scan_with_filter() 
            return self.get_cost_description_sequential_scan() 
        
        return 'Unfortunately, the portion of operation is beyond the scope of this project...'
    
    def get_cost_description_sequential_scan(self): 
        cpu_tuple_cost = self.db.get_cpu_tuple_cost()
        row_count = self.db.get_table_row_count(self.relation_name)
        seq_page_cost = self.db.get_seq_page_cost()
        page_count = self.db.get_table_page_count(self.relation_name)
        startup_cost = 0
        run_cost = (cpu_tuple_cost) * row_count + seq_page_cost * page_count
        total_cost = startup_cost + run_cost 
        valid = abs(total_cost - self.total_cost) <= epsilon
        reason = "WHY? The calculation requires more sophisticated information about DB and these informations are unable to be fetched using query that are more declarative."

        description = f"""
            startup_cost = {startup_cost}
            run_cost = cpu_run_cost + disk_run_cost 
                     = (cpu_tuple_cost) * Ntuple + seq_page_cost * Npage
                     = ({cpu_tuple_cost}) * {row_count} + {seq_page_cost} * {page_count}
                     = {run_cost}
            calculated_total_cost = startup_cost + run_cost 
                                  = {startup_cost} + {run_cost}
                                  = {total_cost}

            psql_total_cost = {self.total_cost}
                                  
            is it a valid calculation? {"YES" if valid else "NO"} (with epsilon = {epsilon})
            {"" if valid else reason}
        """

        return description
    
    def get_cost_description_sequential_scan_with_filter(self): 
        cpu_tuple_cost = self.db.get_cpu_tuple_cost()
        cpu_operator_cost = self.db.get_cpu_operator_cost()
        row_count = self.db.get_table_row_count(self.relation_name)
        seq_page_cost = self.db.get_seq_page_cost()
        page_count = self.db.get_table_page_count(self.relation_name)
        startup_cost = 0
        run_cost = (cpu_tuple_cost + cpu_operator_cost) * row_count + seq_page_cost * page_count
        total_cost = startup_cost + run_cost 
        valid = abs(total_cost - self.total_cost) <= epsilon
        reason = "WHY? The calculation requires more sophisticated information about DB and these informations are unable to be fetched using query that are more declarative."

        description = f"""
            startup_cost = {startup_cost}
            run_cost = cpu_run_cost + disk_run_cost 
                     = (cpu_tuple_cost + cpu_operator_cost) * Ntuple + seq_page_cost * Npage
                     = ({cpu_tuple_cost + cpu_operator_cost}) * {row_count} + {seq_page_cost} * {page_count}
                     = {run_cost}
            calculated_total_cost = startup_cost + run_cost 
                                  = {startup_cost} + {run_cost}
                                  = {total_cost}

            psql_total_cost = {self.total_cost}

            is it a valid calculation? {"YES" if valid else "NO"} (with epsilon = {epsilon})
            {"" if valid else reason}
        """
        return description
class Graph:    
    def __init__(self, query_plan, db: DB): 
        self.db = db 
        self.root = self.parse_query_plan(query_plan)
    
    def parse_query_plan(self, query_plan):
        node = Node(query_plan, self.db)
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
        self.graphviz.node(node.uuid, node.node_type)

        if node.children: 
            for child in node.children: 
                self.graphviz.node(child.uuid, child.node_type)
                self.graphviz.edge(child.uuid, node.uuid)
                self.parse_graph(child)