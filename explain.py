import psycopg2
import graphviz
import random
from pprint import pp
import math
import re

"""
Class DB is the interface class to interact with the database. 
"""
class DB: 
    """
    Constructor to iniate connection with the database.
    """
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
        self.cpu_index_tuple_cost = self.get_cpu_index_tuple_cost()
        self.random_page_cost = self.get_random_page_cost()
        self.cpu_operator_cost = self.get_cpu_operator_cost()
        self.parallel_setup_cost = self.get_parallel_setup_cost()
        self.parallel_tuple_cost = self.get_parallel_tuple_cost()
        self.statistics = self.get_statistics()
        self.work_mem = self.get_work_mem()

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

    """
    Method to close the connection to the database.
    """
    def close_connection(self):
        self.cursor.close()
        self.connection.close()
        
    """
    Method to reset the connection to the database.
    """
    def reset_connection(self):
        self.connection = psycopg2.connect(host=self.host, port=self.port, database=self.database, user=self.user, password=self.password)
        self.cursor = self.connection.cursor()
        
    """
    Method to get the query plan of a given query.
    """
    def get_query_plan(self, query: str): 
        query_plan = self.execute("EXPLAIN (FORMAT JSON, VERBOSE TRUE, BUFFERS TRUE, ANALYZE TRUE) " + query)[0][0][0][0]['Plan']
        return query_plan

    """ 
    Method to get the cpu_tuple_cost
    """
    def get_cpu_tuple_cost(self):
        return float(self.execute("""
                show cpu_tuple_cost;
            """)[0][0][0])

    """
    Method to get the block size of the database.
    """
    def get_block_size(self): 
        return int(self.execute("""
                select current_setting('block_size');
            """)[0][0][0])
    
    """
    Method to get the seq_page_cost of the database.
    """
    def get_seq_page_cost(self):
        return float(self.execute("""
                show seq_page_cost;
            """)[0][0][0])
    
    """
    Method to get the random_page_cost of the database.
    """
    def get_random_page_cost(self):
        return float(self.execute("""
                show random_page_cost;
            """)[0][0][0])
    
    """
    Method to get the cpu_operator_cost of the database.
    """
    def get_cpu_operator_cost(self): 
        return float(self.execute("""
                show cpu_operator_cost;
            """)[0][0][0])
    
    """
    Method to get the parallel_setup_cost of the database.
    """
    def get_parallel_setup_cost(self): 
        return float(self.execute("""
                show parallel_setup_cost;
            """)[0][0][0])
    
    """
    Method to get the parallel_tuple_cost of the database.
    """
    def get_parallel_tuple_cost(self): 
        return float(self.execute("""
                show parallel_tuple_cost;
            """)[0][0][0])
    
    """
    Method to get the cpu_index_tuple_cost of the database.
    """
    def get_cpu_index_tuple_cost(self): 
        return float(self.execute("""
                show cpu_index_tuple_cost
            """)[0][0][0])

    """
    Method to get the table statistics of a given table. Column names can be specified to get the statistics of only specific columns.
    """
    def get_table_statistics(self, table_name, column_names = None): 
        query_results = self.execute("""
            SELECT {column_names} FROM pg_class WHERE relname = '{table_name}';
            """.format(table_name=table_name, column_names="*" if not column_names else ','.join(column_names)))

        table_statistics = {} 
        for column_value, column_name in zip(query_results[0][0], query_results[1]):
            table_statistics[column_name] = column_value
        
        return table_statistics
    
    """
    Method to get the number of pages of a given table.
    """
    def get_table_page_count(self, table_name): 
        return self.statistics[table_name]['relpages']
    
    """
    Method to get the number of tuples of a given table.
    """
    def get_table_row_count(self, table_name): 
        return self.statistics[table_name]['reltuples']

    """
    Method to execute a query.
    """
    def execute(self, query: str):
        self.cursor.execute(query)
        column_names = [description[0] for description in self.cursor.description]
        query_results = self.cursor.fetchall()
        return query_results, column_names

    """
    Method to check the validity of a query.
    """
    def is_query_valid(self, query: str):    
        try:
            self.cursor.execute(query)
            self.cursor.fetchone()
        except Exception as exception:
            return False, exception
        
        return True, None

    """
    Method to get the table names of the database.
    """
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

    """
    Method to get the column names of a given table.
    """
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

    """
    Method to get the overall statistics of a database.
    """
    def get_statistics(self): 
        statistics = {} 
        for table_name in self.get_table_names(): 
            statistics[table_name] = self.get_table_statistics(table_name)
        
        return statistics 
    
    """
    Method to get the work_mem of the database.
    """
    def get_work_mem(self):
        return int(self.execute("""
            SELECT setting::bigint * CASE unit
                WHEN 'kB' THEN 1024
                WHEN 'MB' THEN 1024^2
                ELSE 1
                END AS work_mem_bytes
            FROM pg_settings
            WHERE name = 'work_mem';
        """)[0][0][0])

"""
Class Node is the class to represent a node in the physical query plan.
"""
class Node: 
    """
    Constructor to instantiate a Node object.
    """
    def __init__(self, query_plan, db: DB, children, epsilon): 
        self.query_plan = query_plan
        self.db = db 
        self.uuid = str(random.random())
        self.node_type = query_plan['Node Type']
        self.startup_cost = query_plan['Startup Cost']
        self.acutal_row_count = query_plan['Actual Rows'] if 'Actual Rows' in query_plan else ""
        self.total_cost = query_plan['Total Cost']
        self.row_count = query_plan['Plan Rows']
        self.row_width = query_plan['Plan Width']
        self.output = query_plan['Output'] if 'Output' in query_plan else ""
        self.filter = query_plan['Filter'] if 'Filter' in query_plan else ""
        self.relation_name = query_plan['Relation Name'] if 'Relation Name' in query_plan else ""
        self.workers = query_plan['Workers Planned'] if 'Workers Planned' in query_plan else ""
        self.strategy = query_plan['Strategy'] if 'Strategy' in query_plan else ""
        self.hash_condition = query_plan['Hash Cond'] if 'Hash Cond' in query_plan else ""
        self.children = children
        self.epsilon = epsilon
        self.valid = False
        self.cost_description = self.get_cost_description() 

    """
    Method to get the label for the graph visualization for each node.
    """
    def get_label(self): 
        return f"""{self.node_type + (" with filter " if self.filter else "")} {"- " + self.relation_name if self.relation_name else ""}\n{"cost: " + str(round(self.total_cost, 3))}"""

    """
    Method to get the cost description for each node. For different node_type, we have different cost description function. 
    This function acts as the general function to call the specific cost description function based on the node_type.
    """
    def get_cost_description(self): 
        if self.node_type == 'Seq Scan':
            if self.filter: 
                return self.get_cost_description_sequential_scan_with_filter() 
            return self.get_cost_description_sequential_scan() 
        elif self.node_type == 'Sort':
            return self.get_cost_description_sort()
        elif self.node_type == 'Merge Join':
            return self.get_cost_description_merge_join()
        elif self.node_type == 'Hash':
            return self.get_cost_description_hash() 
        elif self.node_type == 'Aggregate':
            return self.get_cost_description_aggregate() 
        elif self.node_type == 'Hash Join':
            return self.get_cost_description_hash_join() 
        elif self.node_type == 'Gather':
            return self.get_cost_description_gather()
        elif self.node_type == 'Gather Merge':
            return self.get_cost_description_gather_merge() 
        elif self.node_type == 'Index Scan': 
            return self.get_cost_description_index_scan()
        elif self.node_type == 'Materialize':
            return self.get_cost_description_materialize()
        elif self.node_type == 'Nested Loop':
            return self.get_cost_description_nested_loop()
        return f'Unfortunately, the operation of type {self.node_type} is beyond the scope of this project.'
    
    """
    Method to get the cost of sequential scan. 
    We combine what we learnt from the lecture and the PostgreSQL documentation to calculate the cost of the sequential scan by applyin appropriate weight. 
    """
    def get_cost_description_sequential_scan(self): 
        cpu_tuple_cost = self.db.cpu_tuple_cost
        row_count = self.row_count
        seq_page_cost = self.db.seq_page_cost
        page_count = self.db.get_table_page_count(self.relation_name)
        startup_cost = 0
        run_cost = (cpu_tuple_cost) * row_count + seq_page_cost * page_count
        total_cost = startup_cost + run_cost 
        self.valid = abs(total_cost - self.total_cost) <= self.epsilon

        underestimate_reason = """
           The answer is underestimated due to the lack of information to the details needed to calculatae the intricate costs in Postgres.
        """

        overestimate_reason = """
            The answer is overestimated due to Postgres implementing parallel scan, which is not considered in our calculation and not accounted in the lecture formula.
        """

        description = f"""
        startup_cost = {startup_cost} (the cost to retrieve the first row)

        run_cost = cpu_run_cost + disk_run_cost 
                 = (cpu_tuple_cost) * Ntuple + seq_page_cost * Npage
                 = ({cpu_tuple_cost}) * {row_count} + {seq_page_cost} * {page_count}
                 = {run_cost}

        total_cost = startup_cost + run_cost 
                              = {startup_cost} + {run_cost}
                              = {total_cost}

        psql_total_cost = {self.total_cost}
                                
        Is it a valid calculation? {"YES" if self.valid else "NO"} (with epsilon = {self.epsilon})
        {"" if self.valid else underestimate_reason if total_cost <= self.total_cost else overestimate_reason}
        """

        return description
    
    
    """
    Method to get the cost of sequential scan with filter. 
    Similar to get_cost_description_sequential_scan() method, yet we need to consider the filter condition in the cost calculation by adding the term cpu_operator_cost * number_of_input_tuples.
    """
    def get_cost_description_sequential_scan_with_filter(self): 
        cpu_tuple_cost = self.db.cpu_tuple_cost
        cpu_operator_cost = self.db.cpu_operator_cost
        row_count = self.db.get_table_row_count(self.relation_name)
        seq_page_cost = self.db.seq_page_cost
        page_count = self.db.get_table_page_count(self.relation_name)
        startup_cost = 0
        run_cost = (cpu_tuple_cost + cpu_operator_cost) * row_count + seq_page_cost * page_count
        total_cost = startup_cost + run_cost 
        self.valid = abs(total_cost - self.total_cost) <= self.epsilon

        underestimate_reason = """
            The answer is underestimate due to the lack of information to the details needed to calculatae the intricate costs in Postgres.
        """

        overestimate_reason = """
            The answer is overestimated due to Postgres implementing parallel scan, which is not considered in our calculation and not accounted in the lecture formula.
        """


        description = f"""
        startup_cost = {startup_cost} (the cost to retrieve the first row)


        run_cost = cpu_run_cost + disk_run_cost 
                 = (cpu_tuple_cost + cpu_operator_cost) * Ntuple + seq_page_cost * Npage
                 = ({cpu_tuple_cost + cpu_operator_cost}) * {row_count} + {seq_page_cost} * {page_count}
                 = {run_cost}

        total_cost = startup_cost + run_cost 
                              = {startup_cost} + {run_cost}
                              = {total_cost}

        psql_total_cost = {self.total_cost}

        Is it a valid calculation? {"YES" if self.valid else "NO"} (with epsilon = {self.epsilon})
        {"" if self.valid else underestimate_reason if total_cost <= self.total_cost else overestimate_reason}
        """
        return description

    """
    Method to get the cost of sort operation. 
    For the startup_cost and run_cost, we mimic the implementation of PostgreSQL. 
    """
    def get_cost_description_sort(self):
        cpu_operator_cost = self.db.cpu_operator_cost 
        comparison_cost = 2 * cpu_operator_cost
        num_input_tuples = self.children[0].row_count # fetch number of tuples returned from the scan operator cost. 
        log_sort_tuples = math.log2(num_input_tuples)

        last_scan_cost = self.children[0].total_cost # fetch cost of the scan operator. 
        
        startup_cost = last_scan_cost + comparison_cost * num_input_tuples * log_sort_tuples
        run_cost = cpu_operator_cost * num_input_tuples
        total_cost = startup_cost + run_cost
        
        # Confirmation values from EXPLAIN command
        psql_total_cost = self.total_cost  
        self.valid = abs(total_cost - psql_total_cost) <= self.epsilon
        reason = "The calculation may differ due to variations in some cases, such as the output is bigger than the work_mem, which will cause the tuples to be written to disk."

        description = f"""
        startup_cost = {startup_cost}

        run_cost = {cpu_operator_cost} * {num_input_tuples} = {run_cost}
        total_cost = startup_cost + run_cost 
                   = {startup_cost} + {run_cost}
                   = {total_cost}

        psql_total_cost = {psql_total_cost}

        Is it a valid calculation? {"YES" if self.valid else "NO"} (with epsilon = {self.epsilon})
        {"" if self.valid else reason}
        """
        
        return description
    
    """
    Method to get the cost of merge join operation. 
    We estimate the number of blocks my measuring the number of blocks in the smaller relation and the larger relation by using database catalog. 
    blocks = ceil(row_count * row_width / block_size).
    """
    def get_cost_description_merge_join(self):
        """
        - Using 2PMMS join algorithm 3(B(S) + B(R))
        - B(S) = number of blocks in the smaller relation
        - B(R) = number of blocks in the larger relation
        """
        rel_s = self.children[0]
        rel_r = self.children[1]

        b_s = math.ceil(rel_s.row_count * rel_s.row_width / self.db.block_size)
        b_r = math.ceil(rel_r.row_count * rel_r.row_width / self.db.block_size)

        total_cost = 3 * (b_s + b_r) * self.db.seq_page_cost

        self.valid = abs(total_cost - self.total_cost) <= self.epsilon

        reason = f"""
            Our cost is {"underestimated" if total_cost <= self.total_cost else "overestimated"}.
            The calculation done by PostgreSQL is different from ours due to the fact that we are using simple formulas that just count the number of I/O and applying the weights to them. 
            PostgreSQL uses a more sophisticated cost model that incorporates statistics and histograms to estimate the selectivity of the join, which is not obtainable via SQL query.
        """

        description = f"""
            Assumptions:
                - The merge join function cost is using 2PMMS join algorithm
                - Negligible block header

            Using merge join algorithm taught in the lecture 3(num_blocks_S + num_blocks_R)
            num_blocks_S = ceil(row_count_S / row_width_S)
                         = ceil({rel_s.row_count} / {rel_s.row_width})
                         = {b_s}
            
            num_blocks_R = row_count_R / row_width_R
                         = ceil({rel_r.row_count} / {rel_r.row_width})
                         = {b_r}
            
            total_cost = 3 * (num_blocks_S + num_blocks_R) * seq_page_cost
                       = 3 * ({b_s} + {b_r}) * {self.db.seq_page_cost}
                       = {total_cost}

            psql_total_cost = {self.total_cost}

            Is it a valid calculation? {"YES" if self.valid else "NO"} (with epsilon = {self.epsilon})
            {"" if self.valid else reason}
        """

        return description
    
    """
    Method to get nested loop join cost description.
    We have 3 variants for nested loop join: index-based, materialized, and normal nested loop join.
    """
    def get_cost_description_nested_loop(self):
        # compare sizes of 2 input relations. Smaller relation is rel_out and larger relation is rel_in
        if self.children[0].row_count < self.children[1].row_count:
            rel_inner = self.children[1]
            rel_outer = self.children[0]
        else:
            rel_inner = self.children[0]
            rel_outer = self.children[1]

        num_input_tuples_rel_out = rel_outer.row_count
        num_input_tuples_rel_in = rel_inner.row_count

        size_tuple_rel_out = rel_outer.row_width
        size_tuple_rel_in = rel_inner.row_width

        num_blocks_rel_out = math.ceil(size_tuple_rel_out * num_input_tuples_rel_out / self.db.block_size)
        num_blocks_rel_in = math.ceil(size_tuple_rel_in * num_input_tuples_rel_in / self.db.block_size)

        cost_rel_out = rel_outer.total_cost
        cost_rel_in = rel_inner.total_cost

        startup_cost = 0
        run_cost = 0
        description = ""
        underestimate_reason = """
            The answer is underestimate due to the lack of information to the details needed to calculatae the intricate costs in Postgres.
        """

        overestimate_reason = """
            The answer is overestimated due to the way Postgres handle a certain type of relation (e.g. unique inner relation), which they implemented a much more optimized way to handle the join. Thus its cost estimation function is different as well.
        """

        if rel_inner.node_type == 'Materialize' and rel_outer.node_type == 'Seq Scan':

            rescan_cost =  self.db.cpu_operator_cost * size_tuple_rel_in

            run_cost = (self.db.cpu_operator_cost + self.db.cpu_tuple_cost) * num_input_tuples_rel_out * num_input_tuples_rel_in + rescan_cost * (size_tuple_rel_out - 1) + cost_rel_out    
            total_cost = startup_cost + run_cost
           
            description = f"""
                startup_cost = {startup_cost}
                The cost to retrieve the first row is zero

                run_cost = (cpu_operator_cost + cpu_tuple_cost) * num_input_tuples_rel_out * num_input_tuples_rel_in + rescan_cost * (size_tuple_rel_out - 1) + cost_rel_out
                            = ({self.db.cpu_operator_cost} + {self.db.cpu_tuple_cost}) * {num_input_tuples_rel_out} * {num_input_tuples_rel_in} + {rescan_cost} * ({size_tuple_rel_out} - 1) + {cost_rel_out}
                            = {run_cost}
                
                total_cost  = startup_cost + run_cost
                            = {total_cost}

                psql_total_cost = {self.total_cost}

                Valid calculation? {"Yes" if self.valid else "No"}
                {"" if self.valid else underestimate_reason if total_cost <= self.total_cost else overestimate_reason}
            """
        elif rel_inner.node_type == 'Index Scan' and rel_outer.node_type == 'Seq Scan':
            startup_cost = rel_inner.startup_cost
            run_cost = (self.db.cpu_tuple_cost + rel_inner.startup_cost) * num_input_tuples_rel_out + cost_rel_out

            total_cost = startup_cost + run_cost

            description = f"""
                startup_cost = {startup_cost}

                run_cost = (cpu_tuple_cost + startup_cost) * num_input_tuples_rel_out + cost_rel_out
                         = ({self.db.cpu_tuple_cost} + {rel_inner.startup_cost}) * {num_input_tuples_rel_out} + {cost_rel_out}
                         = {run_cost}

                total_cost = startup_cost + run_cost

                psql_total_cost = {self.total_cost}
                
                Valid calculation? {"Yes" if self.valid else "No"}
                {"" if self.valid else underestimate_reason if total_cost <= self.total_cost else overestimate_reason}
            """
        else:
            run_cost = (num_blocks_rel_out + num_blocks_rel_in * num_input_tuples_rel_out) * self.db.seq_page_cost
            total_cost = startup_cost + run_cost
            description = f"""
                Using the lecture's formula,
                
                run_cost = (num_blocks_rel_out + num_blocks_rel_in * num_input_tuples_rel_out) * seq_page_cost
                         = ({num_blocks_rel_out} + {num_blocks_rel_in} * {num_input_tuples_rel_out}) * {self.db.seq_page_cost}
                         = {run_cost}

                total_cost = {startup_cost} + {run_cost}

                psql_total_cost = {self.total_cost}

                Valid calculation? {"Yes" if self.valid else "No"}
                {"" if self.valid else underestimate_reason if total_cost <= self.total_cost else overestimate_reason}
            """ 

        # Confirmation values from EXPLAIN command
        psql_total_cost = self.total_cost  
        self.valid = abs(total_cost - psql_total_cost) <= self.epsilon
        
        return description

    """
    Method to get the cost description of materialize operation.
    """
    def get_cost_description_materialize(self):
        startup_cost = self.children[0].startup_cost
        run_cost = self.children[0].total_cost - self.children[0].startup_cost + 2 * self.db.cpu_operator_cost * self.children[0].row_count

        tuples_size = self.children[0].row_count * self.children[0].row_width

        # If the tuples size > work_mem, then the tuples are written to disk w ceil
        write_to_disk = tuples_size > self.db.work_mem

        extra_run_cost = 0

        if write_to_disk:
            extra_run_cost = self.db.seq_page_cost * math.ceil(tuples_size / self.db.block_size) 

        total_cost = startup_cost + run_cost + extra_run_cost

        # Confirmation values from EXPLAIN command
        psql_total_cost = self.total_cost  
        self.valid = abs(total_cost - psql_total_cost) <= self.epsilon
        
        overestimation_reason = "The answer may differ due to the intricate statistics that cannot be obtained from the query alone."
        underestimation_reason = "The answer is different due to the underestimated size of each tuples which requires intricate statistics (such as byte alignment rule) that cannot be obtained from the query alone."

        reason = overestimation_reason if total_cost > psql_total_cost else underestimation_reason

        extra_description = f"""
            Since the tuples size is greater than work_mem, the tuples are written to disk as the tuples are too large to fit in memory.
            extra_run_cost  = seq_page_cost * ceil(tuples_size / block_size)
            extra_run_cost  = {self.db.seq_page_cost} * ceil({tuples_size} / {self.db.block_size}) = {self.db.seq_page_cost * math.ceil(tuples_size / self.db.block_size)}

            run_cost += extra_run_cost
            run_cost = {run_cost} + {extra_run_cost} 
                     = {run_cost + extra_run_cost}
        """

        description = f"""
            startup_cost = input_startup_cost
                         = {startup_cost}
                            
            run_cost = input_run_cost +  2 * cpu_operator_cost * num_input_tuples
            run_cost = {self.children[0].total_cost - self.children[0].startup_cost} + 2 * {self.db.cpu_operator_cost} * {self.children[0].row_count} 
                     = {run_cost}

            { extra_description if write_to_disk else "" }

            total_cost = start_up_cost + run_cost = {total_cost}

            psql_total_cost = {psql_total_cost}

            Valid calculation? {"Yes" if self.valid else "No"}
            {"" if self.valid else reason}
        """
        return description

    """
    Method to get the cost description of index scan. 
    Getting the exact number of height_of_index in this case is not possible, therefore we calculate the cost as the average of index page access. 
    """
    def get_cost_description_index_scan(self):
        """
        Using the lecture formula:

        avg cost = height of index + data blocks/2 + total blocks/2

        assumptions:
            - branching factor = number of tuples in a block
                               = reltuple / relpages
            - height = log(relpages) / log(branching factor)
            - data blocks = number of tuples / number of tuples in a block * 0.5
        """

        index_relation_name = self.query_plan['Index Name']
        index_statistics = self.db.get_table_statistics(index_relation_name, ['reltuples', 'relpages'])
        num_index_pages, num_index_tuples = index_statistics['relpages'], index_statistics['reltuples']

        row_count = self.db.get_table_row_count(self.relation_name)
        branching_factor = num_index_tuples / num_index_pages
        height_of_index = math.log(num_index_pages) / math.log(branching_factor)
        avg_data_blocks = row_count / branching_factor * 0.5
        avg_cost = (height_of_index + avg_data_blocks + self.db.get_table_page_count(self.relation_name) / 2) * self.db.random_page_cost

        # Confirmation values from EXPLAIN command
        psql_total_cost = self.total_cost  
        self.valid = abs(avg_cost - psql_total_cost) <= self.epsilon
        reason = f"""
            Our cost is {"underestimated" if avg_cost <= psql_total_cost else "overestimated"}.
            The calculation from the EXPLAIN query differs from our calculation due to the limited information provided by the database interface.
            PostgreSQL uses data from histograms and statistics to estimate the selectivity, which the data is not available through SQL query.
            Thus, it is impossible to obtain the exact cost of the index scan operation using the SQL query alone.
        """

        description = f"""
            As there are various types of indexes in PostgreSQL, there will be several assumptions being made:
            - The index is a B+ tree index
            - The given index is clustered index

            branching_factor = num_index_tuples / num_index_pages
                             = {num_index_tuples} / {num_index_pages}
                             = {num_index_tuples / num_index_pages} (number of branch = number of tuples in a block)

            height_of_index = log(num_index_pages) / log(branching_factor)
                            = log({num_index_pages}) / log({branching_factor})
                            = {math.log(num_index_pages) / math.log(branching_factor)}

            avg_data_blocks = row_count / branching_factor * 0.5
                            = {row_count} / {branching_factor} * 0.5
                            = {row_count / branching_factor * 0.5}

            avg_cost = (height_of_index + avg_data_blocks + rel_pages / 2) * random_page_cost
                     = {height_of_index} + {row_count / branching_factor * 0.5} + {self.db.get_table_page_count(self.relation_name) / 2} * {self.db.random_page_cost}
                     = {avg_cost}
                                
            total_cost = {avg_cost}

            psql_total_cost = {psql_total_cost}

            Valid calculation? {"Yes" if self.valid else "No"}
            {"" if self.valid else reason}
            """
        return description
    
    """
    Method to get the cost description of aggregate. 
    We mimic the implementation of PostgreSQL to calculate the cost of the aggregate operation.
    """
    def get_cost_description_aggregate(self): 
        cpu_tuple_cost = self.db.cpu_tuple_cost
        cpu_operator_cost = self.db.cpu_operator_cost
        prev_totalcost = self.children[0].total_cost
        estimated_rows = self.children[0].row_count
        actual_row_count = self.acutal_row_count
        total_cost = prev_totalcost + (estimated_rows * cpu_operator_cost) + (actual_row_count * cpu_tuple_cost)

        psql_total_cost = self.total_cost
        reason = f"""
            Our cost is {"underestimated" if total_cost <= psql_total_cost else "overestimated"}.
            The strategy of the aggregate is different hence the calculation requires more sophisticated information about DB and these informations are unable to be fetched using query that are more declarative.
        """
        formula = f"""
            total_cost = (cost of Seq Scan) + (estimated rows processed * cpu_operator_cost) + (estimated rows returned * cpu_tuple_cost)
                       = ({prev_totalcost}) + ({estimated_rows} * {cpu_operator_cost}) + ({actual_row_count} * {cpu_tuple_cost}) 
                       = {total_cost}
        """

        self.valid = abs(total_cost - self.total_cost) <= self.epsilon
        description = f"""
            {formula}
            psql_total_cost = {psql_total_cost}
            is it a valid calculation? {"YES" if self.valid else "NO"} (with epsilon = {self.epsilon})
            {"" if self.valid else reason}
        """
        return description

    """
    Method to get the cost description of hash. 
    We mimic the implementation of PostgreSQL to calculate the cost of the hash operation.
    """
    def get_cost_description_hash(self): 
        total_cost = self.children[0].total_cost
        psql_total_cost = self.total_cost  
        self.valid = abs(total_cost - self.total_cost) <= self.epsilon
        reason = f"""
            Our cost is {"underestimated" if total_cost <= self.total_cost else "overestimated"}.
            The calculation requires more statistics that are not available outside of external PostgreSQL codebase. 
        """

        description = f"""
            As observed in PostgresSQL, hash cost are passed hence we will do the same.
            total_cost = prev_total_cost
                       = {total_cost}
            PostgreSQL total_cost = {psql_total_cost}
            is it a valid calculation? {"YES" if self.valid else "NO"} (with epsilon = {self.epsilon})
            {"" if self.valid else reason}
        """
        return description
    
    """
    Method to get the cost description of hash join.
    We apply the knowledge from the lecture to calculate the cost of the hash join operation, weighted by the seq_page_cost. 
    """
    def get_cost_description_hash_join(self):
        """
        - Using grace hash join algorithm 3(B(S) + B(R))
        - B(S) = number of blocks in the smaller relation
        - B(R) = number of blocks in the larger relation
        """
        rel_s = self.children[0]
        rel_r = self.children[1]

        b_s = math.ceil(rel_s.row_count * rel_s.row_width / self.db.block_size)
        b_r = math.ceil(rel_r.row_count * rel_r.row_width / self.db.block_size)

        total_cost = 3 * (b_s + b_r) * self.db.seq_page_cost

        self.valid = abs(total_cost - self.total_cost) <= self.epsilon

        reason = f"""
            Our cost is {"underestimated" if total_cost <= self.total_cost else "overestimated"}.
            The calculation done by PostgreSQL is different from ours due to the hash join can be done in multiple batches, it incorporates statistics (for example most common values) - which is not obtainable via SQL query. 
            It also uses a different cost model (hash_qual_cost and qp_qual_cost) to estimate the cost, which causes our estimate to be far below than PostgreSQL's.
        """

        description = f"""
            Assumptions:
                - The hash join function cost is using grace hash join algorithm
                - The smaller hash partition fits in memory
                - Negligible block header

            Using grace hash join algorithm taught in the lecture 3(num_blocks_S + num_blocks_R)
            num_blocks_S = ceil(row_count_S / row_width_S)
                         = ceil({rel_s.row_count} / {rel_s.row_width})
                         = {b_s}
            
            num_blocks_R = row_count_R / row_width_R
                         = ceil({rel_r.row_count} / {rel_r.row_width})
                         = {b_r}
            
            total_cost = 3 * (num_blocks_S + num_blocks_R) * seq_page_cost
                       = 3 * ({b_s} + {b_r}) * {self.db.seq_page_cost}
                       = {total_cost}

            psql_total_cost = {self.total_cost}
                       
            Is it a valid calculation? {"YES" if self.valid else "NO"} (with epsilon = {self.epsilon})
            {"" if self.valid else reason}
        """

        return description
    
    """
    Method to get the cost description of gather operation. 
    We mimic the imlpeentation of PostgreSQL to calculate the cost of the gather operation.
    """
    def get_cost_description_gather(self): 
        parallel_setup_cost = self.db.parallel_setup_cost
        parallel_tuple_cost = self.db.parallel_tuple_cost
        prev_startup_cost = self.children[0].startup_cost
        prev_total_cost = self.children[0].total_cost
        planned_row = self.row_count
        startup_cost = prev_startup_cost + parallel_setup_cost
        run_cost = (prev_total_cost - prev_startup_cost) + (parallel_tuple_cost * self.row_count)
        total_cost = startup_cost + run_cost
        psql_total_cost = self.total_cost  
        self.valid = abs(total_cost - self.total_cost) <= self.epsilon
        reason = f"""
            Our cost is {"underestimated" if total_cost <= self.total_cost else "overestimated"}.
            The calculation requires more statistics that are not available outside of external PostgreSQL codebase. 
        """

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

            psql_total_cost = {psql_total_cost}

            is it a valid calculation? {"YES" if self.valid else "NO"} (with epsilon = {self.epsilon})
            {"" if self.valid else reason}
        """
        return description
    
    """
    Method to get the cost description of gather merge operation.
    We mimic the imlpeentation of PostgreSQL to calculate the cost of the gather merge operation.
    """
    def get_cost_description_gather_merge(self): 
        cpu_operator_cost = self.db.cpu_operator_cost
        parallel_setup_cost = self.db.parallel_setup_cost
        parallel_tuple_cost = self.db.parallel_tuple_cost
        prev_startup_cost = self.children[0].startup_cost
        workers = self.workers
        planned_row = self.row_count

        n = workers + 1
        logN = math.log2(n)
        comparison_cost = 2.0 * cpu_operator_cost

        startup_cost = (comparison_cost * n * logN) + parallel_setup_cost + prev_startup_cost
        run_cost = (planned_row * comparison_cost * logN) + (cpu_operator_cost * planned_row) + (parallel_tuple_cost * planned_row * 1.05)

        total_cost = startup_cost + run_cost
        psql_total_cost = self.total_cost  
        self.valid = abs(total_cost - self.total_cost) <= self.epsilon
        reason = f"""
            Our cost is {"underestimated" if total_cost <= self.total_cost else "overestimated"}.
            The calculation requires more statistics that are not available outside of external PostgreSQL codebase. 
        """

        description = f"""
            n = workers + 1
                = {workers} + 1
                = {n}
            comparison_cost = 2.0 * cpu_operator_cost
                = 2.0 * {cpu_operator_cost}
                = {comparison_cost}
            startup_cost = (comparison_cost * n * logN) + parallel_setup_cost + prev_startup_cost
                = ({comparison_cost} * {n} * {logN}) + {parallel_setup_cost + prev_startup_cost}
                = {startup_cost}
            run_cost = (planned_row * comparison_cost * logN) + (cpu_operator_cost * planned_row) + (parallel_tuple_cost * planned_row * 1.05)
                = ({planned_row} * {comparison_cost} * {logN}) + ({cpu_operator_cost} * {planned_row}) + ({parallel_tuple_cost} * {planned_row} * 1.05)
                = {run_cost}

            total cost = (startup_cost) + (run_cost)
                = {startup_cost} + {run_cost}
                = {total_cost}

            psql_total_cost = {psql_total_cost}

            is it a valid calculation? {"YES" if self.valid else "NO"} (with epsilon = {self.epsilon})
            {"" if self.valid else reason}
        """
        return description

"""
Class Graph is a class to represent the whole graph of the physical query plan. 
This serves as a wrapper class to parse the query plan and create the graph.
"""
class Graph:    
    """
    Constructor to instantiate a Graph object.
    """
    def __init__(self, query_plan, db: DB, epsilon): 
        self.db = db 
        self.epsilon = epsilon
        self.root = self.parse_query_plan(query_plan)
    
    """
    Method to parse the query plan and create the graph.
    """
    def parse_query_plan(self, query_plan):
        children = []
        if 'Plans' in query_plan: 
            for child_query_plan in query_plan['Plans']: 
                children.append(self.parse_query_plan(child_query_plan)) 

        node = Node(query_plan, self.db, children, self.epsilon)
        return node 

"""
Class GraphVisualizer is a class to visualize the graph of the physical query plan by parsing the Graph object. 
It leverages graphviz library to create the visualization of the graph
"""
class GraphVisualizer: 
    """
    Constructor to instantiate a GraphVisualizer object.
    """
    def __init__(self, graph):
        self.graphviz = graphviz.Digraph('G', filename='qep', format='png')
        self.graphviz.attr(rankdir='BT')
        self.parse_graph(graph.root)
        self.graphviz.render('assets/img/qep')

    """
    Method to parse the graph and create the visualization.
    """
    def parse_graph(self, node: Node):
        if not node.valid: 
            self.graphviz.node(node.uuid, node.get_label(), fillcolor='cyan', style='filled')
        else: 
            self.graphviz.node(node.uuid, node.get_label(), fillcolor='green', style='filled')
            
        if node.children: 
            for child in node.children: 
                self.graphviz.node(child.uuid, child.node_type)
                self.graphviz.edge(child.uuid, node.uuid)
                self.parse_graph(child)