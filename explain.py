import psycopg2
class DB: 
    def __init__(self, config): 
        self.host = config['host']
        self.port = config['port']
        self.database = config['database']
        self.user = config['user']
        self.password = config['password']
        self.connection = psycopg2.connect(host=self.host, port=self.port, database=self.database, user=self.user, password=self.password)
        self.cursor = self.connection.cursor()

    def get_query_plan(self, query): 
        query_plan = self.execute("EXPLAIN (FORMAT JSON) " + query)[0][0][0][0]['Plan']
        return query_plan

    def get_query_plan_analysis(self, query): 
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

    def get_all_table_names(self): 
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

    def get_distinct_rows_count(self, table_name, column_name): 
        query_results = self.execute(
            """
            SELECT 
                COUNT(DISTINCT({column_name})) 
                FROM {table_name}
            """.format(column_name=column_name, table_name=table_name)
        )

        return query_results[0][0][0]
    
    def get_rows_count(self, table_name): 
        query_results = self.execute(
            """
            SELECT 
                COUNT(*) 
                FROM {table_name}
            """.format(table_name)
        )

        return query_results[0][0][0]