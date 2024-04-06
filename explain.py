import psycopg2

EXPLAIN_PROMPT = "EXPLAIN (FORMAT JSON) "
EXPLAIN_ANALYZE_PROMPT = "EXPLAIN ANALYZE "

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
        query_plan = self.execute(EXPLAIN_PROMPT + query)[0][0][0][0]['Plan']
        return query_plan

    def get_query_plan_analysis(self, query): 
        query_plan_analysis = self.execute(EXPLAIN_ANALYZE_PROMPT + query)
        return query_plan_analysis 

    def execute(self, query: str):
        self.cursor.execute(query)
        columns = [description[0] for description in self.cursor.description]
        query_results = self.cursor.fetchall()
        return query_results, columns

    def is_query_valid(self, query: str):    
        try:
            self.cursor.execute(query)
            self.cursor.fetchone()
        except Exception as exception:
            return False, exception
        
        return True, None