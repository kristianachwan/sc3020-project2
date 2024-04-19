import psycopg2
import graphviz
import random
from pprint import pp
import math

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
        self.parallel_setup_cost = self.get_parallel_setup_cost()
        self.parallel_tuple_cost = self.get_parallel_tuple_cost()
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
    
    def get_parallel_setup_cost(self): 
        return float(self.execute("""
                show parallel_setup_cost;
            """)[0][0][0])
    
    def get_parallel_tuple_cost(self): 
        return float(self.execute("""
                show parallel_tuple_cost;
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
    def __init__(self, query_plan, db: DB, children = []): 
        self.db = db 
        self.uuid = str(random.random())
        self.node_type = query_plan['Node Type']
        self.startup_cost = query_plan['Startup Cost']
        self.acutal_row_count = query_plan['Actual Rows'] if 'Actual Rows' in query_plan else ""
        self.total_cost = query_plan['Total Cost']
        self.row_count = query_plan['Plan Rows']
        self.output = query_plan['Output'] if 'Output' in query_plan else ""
        self.filter = query_plan['Filter'] if 'Filter' in query_plan else ""
        self.relation_name = query_plan['Relation Name'] if 'Relation Name' in query_plan else ""
        self.children = children
        self.cost_description = self.get_cost_description() 
        
    def get_cost_description(self): 
        if self.node_type == 'Seq Scan':
            if self.filter: 
                return self.get_cost_description_sequential_scan_with_filter() 
            return self.get_cost_description_sequential_scan() 
        elif self.node_type == 'Sort':
            return self.get_sort_cost_description()
        elif self.node_type == 'Hash':
            return self.get_cost_description_hash() 
        elif self.node_type == 'Aggregate':
            return self.get_cost_description_aggregate() 
        elif self.node_type == 'Hash Join':
            return self.get_cost_description_hash_join() 
        elif self.node_type == 'Gather':
            return self.get_cost_description_gather() 
        
        return 'Unfortunately, the portion of operation is beyond the scope of this project...'
    
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
            startup_cost = {startup_cost}
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
    
    def get_cost_description_aggregate(self): 
        cpu_tuple_cost = self.db.cpu_tuple_cost
        cpu_operator_cost = self.db.cpu_operator_cost
        prev_cost = self.children[0].total_cost
        estimated_rows = self.children[0].row_count
        actual_row_count = self.acutal_row_count
        total_cost = prev_cost + (estimated_rows * cpu_operator_cost) + (actual_row_count * cpu_tuple_cost)
        valid = abs(total_cost - self.total_cost) <= epsilon
        reason = "WHY? The calculation requires more sophisticated information about DB and these informations are unable to be fetched using query that are more declarative."

        description = f"""
            Total cost of Aggregate
                = (cost of Seq Scan) + (estimated rows processed * cpu_operator_cost) + (estimated rows returned * cpu_tuple_cost)
                = ({prev_cost}) + (1 * {cpu_operator_cost}) + ({actual_row_count} * {cpu_tuple_cost}) 
                = {total_cost}
                is it a valid calculation? {"YES" if valid else "NO"} (with epsilon = {epsilon})
                {"" if valid else reason}
        """
        return description

    def get_cost_description_hash(self): 
        total_cost = self.children[0].total_cost
        valid = abs(total_cost - self.total_cost) <= epsilon
        reason = "WHY? The calculation requires more sophisticated information about DB and these informations are unable to be fetched using query that are more declarative."

        description = f"""
            Total cost of Hash {total_cost}. As observed in PostgresSQL, hash cost are passed hence we will do the same.
            is it a valid calculation? {"YES" if valid else "NO"} (with epsilon = {epsilon})
            {"" if valid else reason}
        """
        return description
    
    def get_cost_description_hash_join(self): 
        total_cost = 3 * 0

        description = f"""
            We will be using the formula of Grace Hash Join taught in lecture here: 3(B(R) + B(S))
            Total cost of Hash Join {total_cost}
        """
        return description
    
    def get_cost_description_gather(self): 
        parallel_setup_cost = self.db.parallel_setup_cost
        parallel_tuple_cost = self.db.parallel_tuple_cost
        prev_startup_cost = self.children[0].startup_cost
        prev_total_cost = self.children[0].total_cost
        planned_row = self.row_count
        startup_cost = prev_startup_cost + parallel_setup_cost
        run_cost = (prev_total_cost - prev_startup_cost) + (parallel_tuple_cost * self.row_count)
        total_cost = startup_cost + run_cost
        valid = abs(total_cost - self.total_cost) <= epsilon
        reason = "WHY? The calculation requires more sophisticated information about DB and these informations are unable to be fetched using query that are more declarative."

        description = f"""
            Total cost of Gather
             startup_cost = startup_cost + parallel_setup_cost
                = {prev_startup_cost} + {parallel_setup_cost}
                = {startup_cost}
             run_cost = (prev_total_cost - prev_startup_cost) + (parallel_tuple_cost * planned_row)
                = ({prev_total_cost} - {prev_startup_cost}) + ({parallel_tuple_cost} * {planned_row})
                = {run_cost}
             total cost = (startup_cost) + (run_cost)
                = ({startup_cost} + {run_cost})
                = {total_cost}
            is it a valid calculation? {"YES" if valid else "NO"} (with epsilon = {epsilon})
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