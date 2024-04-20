import psycopg2
import graphviz
import random
from pprint import pp
import math
import re

# this is permissible error from the estimation
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
        self.block_size = self.get_block_size()
        self.seq_page_cost = self.get_seq_page_cost()
        self.cpu_tuple_cost = self.get_cpu_tuple_cost() 
        self.random_page_cost = self.get_random_page_cost()
        self.cpu_operator_cost = self.get_cpu_operator_cost()
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

    def get_block_size(self): 
        return int(self.execute("""
                select current_setting('block_size');
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
    
    def get_index_pages(self):
        pass

class Node: 
    def __init__(self, query_plan, db: DB, children = []): 
        self.query_plan = query_plan
        self.db = db 
        self.uuid = str(random.random())
        self.node_type = query_plan['Node Type']
        self.startup_cost = query_plan['Startup Cost']
        self.total_cost = query_plan['Total Cost']
        self.row_count = query_plan['Plan Rows']
        self.output = query_plan['Output']
        self.filter = query_plan['Filter'] if 'Filter' in query_plan else ""
        self.relation_name = query_plan['Relation Name'] if 'Relation Name' in query_plan else ""
        self.children = children
        self.cost_description = self.get_cost_description()
        # self.index_name = self.get_index(query_plan)

    def get_index(self):
        if self.query_plan['Node Type'] == 'Index Scan':
            # print("INDEX: ", query_plan['Index Name'])
            # return query_plan['Index Name']
            query_plan_str = str(self.query_plan)
            # Regular expression pattern to match 'Index Name'
            pattern = r"'Index Name': '([^']+)'"
            # Search for the pattern in the input string
            match = re.search(pattern, query_plan_str)

            # Extract the 'Index Name' value if found
            if match:
                index_name = match.group(1)
                print("Index Name:", index_name)
                return index_name
            else:
                print("Index Name not found.")
                return None
        else:
            return None
        
    def get_cost_description(self): 
        if self.node_type == 'Seq Scan':
            if self.filter: 
                return self.get_cost_description_sequential_scan_with_filter() 
            return self.get_cost_description_sequential_scan() 
        elif self.node_type == 'Sort':
            return self.get_sort_cost_description()
        elif self.node_type == 'Nested Loop':
            return self.get_nested_loop_join_description()
        elif self.node_type == 'Index Scan':
            return self.get_index_scan_description()
        
        return 'Unfortunately, the portion of operation is beyond the scope of this project...'
    
    def get_index_pages_tuples(self, idx):
        query = f"SELECT relpages, reltuples FROM pg_class WHERE relname = '{idx}';"
        results = self.db.execute(query)
        num_index_pages = results[0][0][0]
        num_index_tuples = results[0][0][1]
        # print("RESULTS: ", results, num_index_pages, num_index_tuples)
        return num_index_pages, num_index_tuples
    
    def get_cost_description_sequential_scan(self): 
        cpu_tuple_cost = self.db.cpu_tuple_cost
        row_count = self.db.get_table_row_count(self.relation_name)
        seq_page_cost = self.db.seq_page_cost
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
        cpu_tuple_cost = self.db.cpu_tuple_cost
        cpu_operator_cost = self.db.cpu_operator_cost
        row_count = self.db.get_table_row_count(self.relation_name)
        seq_page_cost = self.db.seq_page_cost
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
    
    # Sort cost = start_up cost + run cost
    # start_up cost = cost_of_last_scan + 2 * cpu_operator_cost * number_of_input_tuples * log2(number_of_input_tuples)
        # cost_of_last_scan -> can write function to fetch it. For now just assume some constant value
        # cpu_operator_cost -> db.cpu_operator_cost or default value is 0.0025
        # number_of_input_tuples -> fetch 'rows' attribute of sequential scan
    # run cost = cpu_operator_cost *  number_of_input_tuples
    def get_sort_cost_description(self):
        cpu_operator_cost = self.db.cpu_operator_cost # if this doesn't work then the default value is 0.0025
        comparison_cost = 2 * cpu_operator_cost
        num_input_tuples = self.children[0].row_count # fetch number of tuples returned from the scan operator cost. 
        log_sort_tuples = math.log2(num_input_tuples)

        last_scan_cost = self.children[0].total_cost # fetch cost of the scan operator. 
        
        startup_cost = last_scan_cost + comparison_cost * num_input_tuples * log_sort_tuples
        run_cost = cpu_operator_cost * num_input_tuples
        total_cost = startup_cost + run_cost
        
        # Confirmation values from EXPLAIN command
        psql_total_cost = self.total_cost  
        valid = abs(total_cost - psql_total_cost) <= epsilon
        reason = "The calculation may differ due to variations in system configurations or PostgreSQL versions."

        description = f"""
            startup_cost = cost_of_last_scan + 2 * cpu_operator_cost * number_of_input_tuples * log2(number_of_input_tuples)
            startup_cost = {startup_cost}
            run_cost = run cost = cpu_operator_cost *  number_of_input_tuples
            run_cost = {cpu_operator_cost} * {num_input_tuples} = {run_cost}
            total_cost = startup_cost + run_cost = {total_cost}
            PostgreSQL total_cost = {psql_total_cost}
            Valid calculation? {"Yes" if valid else "No"}
            {"" if valid else reason}
        """
        return description
    
    # Sort merge join cost = start_up cost + run cost
    # startup_cost = num_input_tuples_rel_out*log2(num_input_tuples_rel_in) + num_input_tuples_rel_in * log2(num_input_tuples_rel_out)
    # run cost = num_input_tuples_rel_in + num_input_tuples_rel_out
    def get_sort_merge_join_description(self):
        # compare sizes of 2 input relations. Smaller relation is rel_out and larger relation is rel_in
        if self.children[0].row_count < self.children[1].row_count:
            rel_inner = self.children[1]
            rel_outer = self.children[0]
        else:
            rel_inner = self.children[0]
            rel_outer = self.children[1]

        # need to define a function to fetch number of tuples from the can of rel_out and rel_in
        num_input_tuples_rel_out = rel_outer.row_count
        num_input_tuples_rel_in = rel_inner.row_count

        startup_cost = num_input_tuples_rel_out*math.log2(num_input_tuples_rel_in) + num_input_tuples_rel_in*math.log2(num_input_tuples_rel_out)

        run_cost = num_input_tuples_rel_in + num_input_tuples_rel_out
        total_cost = startup_cost + run_cost
        
        # Confirmation values from EXPLAIN command
        psql_total_cost = self.total_cost  
        valid = abs(total_cost - psql_total_cost) <= epsilon
        reason = "The calculation may differ due to variations in system configurations or PostgreSQL versions."

        description = f"""
            startup_cost = {startup_cost}
            run_cost = {num_input_tuples_rel_in} + {num_input_tuples_rel_out} = {run_cost}
            total_cost = startup_cost + run_cost = {total_cost}
            PostgreSQL total_cost = {psql_total_cost}
            Valid calculation? {"Yes" if valid else "No"}
            {"" if valid else reason}
        """
        return description
    
    def get_nested_loop_join_description(self):
        # compare sizes of 2 input relations. Smaller relation is rel_out and larger relation is rel_in
        if self.children[0].row_count < self.children[1].row_count:
            rel_inner = self.children[1]
            rel_outer = self.children[0]
        else:
            rel_inner = self.children[0]
            rel_outer = self.children[1]

        # fetch number of tuples from the can of rel_out and rel_in
        num_input_tuples_rel_out = rel_outer.row_count
        num_input_tuples_rel_in = rel_inner.row_count

        # fetch number of tuples from the can of rel_out and rel_in
        cost_rel_out = rel_outer.total_cost
        cost_rel_in = rel_inner.total_cost

        startup_cost = 0

        run_cost = (self.db.cpu_operator_cost + self.db.cpu_operator_cost) * num_input_tuples_rel_out * num_input_tuples_rel_in + cost_rel_in*num_input_tuples_rel_out + cost_rel_out
        total_cost = startup_cost + run_cost
        
        # Confirmation values from EXPLAIN command
        psql_total_cost = self.total_cost  
        valid = abs(total_cost - psql_total_cost) <= epsilon
        reason = "The calculation may differ due to variations in system configurations or PostgreSQL versions."

        description = f"""
            startup_cost = {startup_cost}
            run_cost = {num_input_tuples_rel_in} + {num_input_tuples_rel_out} = {run_cost}
            total_cost = startup_cost + run_cost = {total_cost}
            PostgreSQL total_cost = {psql_total_cost}
            Valid calculation? {"Yes" if valid else "No"}
            {"" if valid else reason}
        """
        return description
    
    def get_materialise_description(self):
        startup_cost = 0
        run_cost = 2 * self.db.cpu_operator_cost * self.children[0].row_count

        total_cost = (startup_cost + self.children[0].total_cost) + run_cost

        # Confirmation values from EXPLAIN command
        psql_total_cost = self.total_cost  
        valid = abs(total_cost - psql_total_cost) <= epsilon
        reason = "The calculation may differ due to variations in system configurations or PostgreSQL versions."

        description = f"""
            startup_cost = 0
            startup_cost = {startup_cost}
            run_cost = 2 * cpu_operator_cost * num_input_tuples
            run_cost = 2 * {self.db.cpu_operator_cost} * {self.children[0].row_count} = {run_cost}
            total_cost = (startup_cost + total_cost_of_scan) + run_cost = {total_cost}
            PostgreSQL total_cost = {psql_total_cost}
            Valid calculation? {"Yes" if valid else "No"}
            {"" if valid else reason}
        """
        return description

    def get_index_scan_description(self):
        idx = self.get_index()
        num_index_pages, num_index_tuples = self.get_index_pages_tuples(idx)
        print("num_index_tuples", num_index_tuples)
        # NEED TO FIND
        index_tree_height = 1

        startup_cost = (math.ceil(math.log2(num_index_tuples)) + (index_tree_height + 1)*50) * self.db.cpu_operator_cost

        page_count = self.db.get_table_page_count(self.relation_name)
        seq_page_cost = self.db.seq_page_cost
        # NEED TO FIND
        selectivity = 0.00001
        qual_op_cost = 0.0025 # Default value 0.0025
        # NEED TO FIND
        indexCorrelation = 1
        max_io_cost =  page_count*self.db.random_page_cost
        min_io_cost = self.db.random_page_cost + (math.ceil(selectivity * page_count)-1) * seq_page_cost
        cpu_index_tuple_cost = 0.005 # cpu_index_tuple_cost by default is 0.005

        index_cpu_cost = selectivity * num_index_tuples * (cpu_index_tuple_cost + qual_op_cost)
        table_cpu_cost = selectivity * self.db.get_table_row_count(self.relation_name) * self.db.cpu_tuple_cost
        index_io_cost = math.ceil(selectivity * num_index_pages) * self.db.random_page_cost
        table_io_cost = max_io_cost + indexCorrelation**2 * (min_io_cost - max_io_cost)

        run_cost = (index_cpu_cost + table_cpu_cost) + (index_io_cost + table_io_cost)

        total_cost = startup_cost + run_cost

        # Confirmation values from EXPLAIN command
        psql_total_cost = self.total_cost  
        valid = abs(total_cost - psql_total_cost) <= epsilon
        reason = "The calculation may differ due to variations in system configurations or PostgreSQL versions."

        description = f"""
            startup_cost = {startup_cost}
            run_cost = {index_cpu_cost} + {table_cpu_cost} + {index_io_cost} + {table_io_cost} = {run_cost}
            total_cost = index_cpu_cost + table_cpu_cost + index_io_cost + table_io_cost = {total_cost}
            PostgreSQL total_cost = {psql_total_cost}
            Valid calculation? {"Yes" if valid else "No"}
            {"" if valid else reason}
        """
        return description
    
class Graph:    
    def __init__(self, query_plan, db: DB): 
        self.db = db 
        self.root = self.parse_query_plan(query_plan)
    
    def parse_query_plan(self, query_plan):
        children = []
        if 'Plans' in query_plan: 
            for child_query_plan in query_plan['Plans']: 
                children.append(self.parse_query_plan(child_query_plan)) 

        node = Node(query_plan, self.db, children)
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
