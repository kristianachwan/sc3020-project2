import ttkbootstrap as ttk
import tkinter as tk
from tkinter import messagebox
from explain import DB, Graph, GraphVisualizer, Node
from PIL import ImageTk, Image

TEXT_PRIMARY_COLOR = "#F9F9F9"
TEXT_SECONDARY_COLOR = "grey"

"""
Class Input is the base class component for the input fields in QUPEX.
"""
class Input(ttk.Entry):
    """
    Constructor to instantaite the Input class.
    """
    def __init__(self, master=None, placeholder="", default_value=None, **kwargs):
        self.is_empty = True
        self.color = TEXT_PRIMARY_COLOR
        self.placeholder_color = TEXT_SECONDARY_COLOR
        self.placeholder = placeholder
        self.show = kwargs.pop("show", "")

        super().__init__(master, 
                         bootstyle="primary",
                        foreground=self.color, 
                        **kwargs)
        self.bind("<FocusIn>", self.foc_in)
        self.bind("<FocusOut>", self.foc_out)
        
        if default_value:
            self.insert(0, default_value)
            self.is_empty = False
        else:
            self.configure(show="")
            self.insert(0, self.placeholder)
            self.configure(foreground=self.placeholder_color)
    
    """
    Method to handle the focus in event.
    """
    def foc_in(self, *args):
        if self.is_empty:
            self.delete("0", "end")
            self.configure(show=self.show)
            self.configure(foreground=self.color)

    """
    Method to handle the focus in event.
    """
    def foc_out(self, *args):
        if not self.get():
            self.configure(show="")
            self.configure(foreground=self.placeholder_color)
            self.insert(0, self.placeholder)
            self.is_empty = True
        else:
            self.is_empty = False

"""
Class InputWithLabel is a component that contains an input field and a label.
"""
class InputWithLabel(ttk.Frame):
    """
    Constructor to instantiate the InputWithLabel class.
    """
    def __init__(self, master=None, label_text="", placeholder="", default_value=None, show="", **kwargs):
        super().__init__(master, **kwargs)
        self.pack()

        self.label = ttk.Label(self, text=label_text, anchor=ttk.W, foreground=TEXT_PRIMARY_COLOR)
        self.label.pack(side = ttk.TOP, fill="x")

        self.entry = Input(self, placeholder=placeholder, default_value=default_value, show=show, )
        self.entry.pack(side = ttk.TOP)         

"""
Class QueryExplanation is a component that contains the query plan treeview and the explanation of the selected node.
"""
class QueryExplanation(ttk.Frame):
    """
    Method to recursively update the treeview.
    """
    def __recursive_update(self, node: Node, parent):
        def callback(event):
            self.selected_node = node
            self.query_explanation.config(state=tk.NORMAL)
            self.query_explanation.delete("1.0", ttk.END)
            self.query_explanation.insert(tk.INSERT, node.cost_description)
            self.query_explanation.config(state=tk.DISABLED)

        curnode = self.query_selection_tree.insert(parent, "end", text=node.node_type, values=(node.startup_cost, node.total_cost, node.row_count), tags=(node.node_type, node.uuid))
        self.query_selection_tree.tag_bind(node.uuid, "<<TreeviewSelect>>", callback=callback)
        for child in node.children:
            self.__recursive_update(child, curnode)

    """
    Method to update the treeview.
    """
    def update_treeview(self, event):
        root: Node = self.master.master.master.master.inner_state.graph.root
        self.query_selection_tree.delete(*self.query_selection_tree.get_children())
        self.__recursive_update(root, "")

    """
    Constructor to instantiate the QueryExplanation class.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pack(expand=True, fill="both")
        self.selected_node = None
        """
        self
        |-> query_selection_frame   
            |-> query_selection (treeview)
        |-> query_explanation_frame
            |-> query_explanation (label)
        """

        self.query_selection_frame = ttk.Frame(self, width=5)
        self.query_selection_frame.pack(side = ttk.LEFT, fill="y", padx=(0, 16))

        self.query_selection_tree = ttk.Treeview(self.query_selection_frame, columns=("startup", "cost", "rows"), height=50)
        self.query_selection_tree.heading("#0", text="Query Plan")
        self.query_selection_tree.column("#0", width=150)
        self.query_selection_tree.heading("#1", text="Startup Cost")
        self.query_selection_tree.column("#1", width=120)
        self.query_selection_tree.heading("#2", text="Total Cost")
        self.query_selection_tree.column("#2", width=120)
        self.query_selection_tree.heading("#3", text="Rows")
        self.query_selection_tree.column("#3", width=80)
        
        # Onclick event (based on tags)
        self.query_selection_tree.pack(side = ttk.LEFT, fill="y")

        self.query_explanation_frame = ttk.Frame(self)
        self.query_explanation_frame.pack(side = ttk.LEFT, fill="both", expand=True)

        explanation = "Click on the table to the left to see the explanation of the query plan."

        self.query_explanation = ttk.ScrolledText(self.query_explanation_frame, wrap="word")
        self.query_explanation.pack(side = ttk.LEFT, fill="y")
        self.query_explanation.insert(tk.INSERT, explanation)
        self.query_explanation.config(state=tk.DISABLED)

"""
Class QueryTable is a component that contains the statistics of the database and the schema of the database.
"""
class QueryTable(ttk.Frame):
    """
    Constructor to instantiate the QueryTable class.    
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pack()

        """
        self
        |-> notebook
            |-> query_table
            |-> schema_table
        """

        self.notebook = ttk.Notebook(self, height=1000)
        self.notebook.pack(side = ttk.TOP, fill="both", expand=True)

        self.query_table = ttk.Frame(self.notebook, width=720, height=1000)
        self.query_table.pack(fill="y")
        db_con: DB = self.master.master.master.master.inner_state.db_connection
        statistic = db_con.get_statistics()

        # Generate table for Relation Statistics
        header = ["relname", "relpages", "reltuples", "relhasindex"]
        content = []
        for val in statistic.values():
            content.append([val[header_keys] for header_keys in header])

        self.table = ttk.Treeview(self.query_table, columns=header, show="headings")
        self.table.pack(fill="both", expand=True)
        self.table.heading("#0", text="")
        self.table.heading("#1", text="Name")
        self.table.column("#1", width=40, anchor=tk.W)
        self.table.heading("#2", text="No. of Pages")
        self.table.column("#2", width=40, anchor=tk.W)
        self.table.heading("#3", text="No. of Tuples")
        self.table.column("#3", width=40, anchor=tk.W)
        self.table.heading("#4", text="Has Index")
        self.table.column("#4", width=25, anchor=tk.W)

        for row in content:
            self.table.insert("", "end", values=row)

        # Generate table for Schemas
        relations = db_con.get_table_names()
        
        self.schema_table_frame = ttk.Frame(self.notebook, width=480, height=1000)
        self.schema_table_frame.pack(fill="y")

        self.schema_table = ttk.Treeview(self.schema_table_frame, columns=["Relation", "Column"], show="tree headings")
        self.schema_table.pack(fill="both", expand=True)

        self.schema_table.column("#0", width=40, anchor=tk.W)
        self.schema_table.heading("#1", text="Relation")
        self.schema_table.column("#1", width=40, anchor=tk.W)
        self.schema_table.heading("#2", text="Column")
        self.schema_table.column("#2", width=40, anchor=tk.W)

        for relation in relations:
            columns = db_con.get_column_names(relation)
            par = self.schema_table.insert("", "end", values=[relation, ""])    
            for column in columns:
                self.schema_table.insert(par, "end", values=["", column])


        self.notebook.add(self.query_table, text="Statistics")
        self.notebook.add(self.schema_table_frame, text="Schemas")

        
"""
Class SQLInput is a component that contains the input field for the SQL query.
"""
class SQLInput(ttk.Frame):
    
    """
    Static variable that contains the SQL keywords for highlighting.
    """
    SQL_KEYWORDS = ["SELECT", "FROM", "WHERE", "GROUP BY", "HAVING", "ORDER BY", "LIMIT", "OFFSET", "JOIN", "INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN", "CROSS JOIN", "NATURAL JOIN", "USING", "ON", "AS", "AND", "OR", "NOT", "IN", "LIKE", "BETWEEN", "IS", "NULL", "EXISTS", "ALL", "ANY", "SOME", "UNION", "INTERSECT", "EXCEPT", "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE", "CREATE", "TABLE", "DROP", "ALTER", "ADD", "PRIMARY KEY", "FOREIGN KEY", "REFERENCES", "INDEX", "UNIQUE", "CHECK", "DEFAULT", "AUTO_INCREMENT", "CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_USER", "DATABASE", "IF", "EXISTS", "THEN", "ELSE", "END", "CASE", "WHEN", "WHILE", "DO", "BEGIN", "DECLARE", "CURSOR", "OPEN", "CLOSE", "FETCH", "LOOP", "EXIT", "CONTINUE", "GOTO", "RETURN", "CALL", "PROCEDURE", "FUNCTION", "TRIGGER", "EVENT", "HANDLER", "REPLACE", "GRANT", "REVOKE", "PRIVILEGES", "WITH", "OPTION", "LOCK", "UNLOCK", "START", "TRANSACTION", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE", "ISOLATION", "LEVEL", "READ", "WRITE", "ONLY", "REPEATABLE", "COMMITTED", "SERIALIZABLE", "AUTOCOMMIT", "SHOW", "STATUS", "VARIABLES", "DATABASES", "TABLES", "INDEXES", "GRANTS", "PROCESSLIST", "KILL", "SHUTDOWN", "LOGS", "ERRORS", "WARNINGS", "SLAVE", "MASTER", "REPLICATION", "BINARY", "LOG", "POSITION", "FILE", "FORMAT", "PASSWORD", "USER", "HOST", "PRIVILEGE", "RELOAD", "FLUSH", "LOGS", "TABLES", "STATISTICS", "QUERY", "CACHE", "MEMORY"] 
    
    """
    Method to execute the query.
    """
    def __execute_query(self, event):
        db: DB = self.master.master.master.master.inner_state.db_connection
        def reset_connection():
            try:
                db.reset_connection()
            except:
                messagebox.showerror("Error", "An error when resetting the connection")
                self.master.master.master.master.inner_state.db_connection = None

                # Refresh
                self.master.master.master.master.refresh_content_layout()
                return
            
        query = self.query_input.get("1.0", "end-1c")
        
        if db.is_query_valid(query)[0] is False:
            messagebox.showerror("Error", "Invalid query")
            reset_connection()
            return 
        
        try:
            query_plan = self.master.master.master.master.inner_state.db_connection.get_query_plan(query) # forgive me
        except:
            messagebox.showerror("Error", "An error has occured during the execution of the query")
            reset_connection()
            return
        
        epsilon = float(self.epsilon_input.get())

        try:
            graph = Graph(query_plan, self.master.master.master.master.inner_state.db_connection, epsilon=epsilon)
            self.master.master.master.master.inner_state.graph = graph

            graphviz = GraphVisualizer(graph)

            self.master.master.master.refresh_query_content()
        except Exception as e:
            messagebox.showerror("Error", str(e))
            reset_connection()
            
        self.master.master.master.query_explanation.update_treeview(None)
    
    """
    Method to highlight the keywords in the query input.
    """
    def highlight_keywords(self, event):
        # Remove all tags
        self.query_input.tag_remove("keyword", "1.0", "end")

        for keyword in SQLInput.SQL_KEYWORDS:
            start_idx = "1.0"
            while True:
                start_idx = self.query_input.search(keyword, start_idx, stopindex="end", nocase=1)
                
                if not start_idx:
                    break
                end_idx = f"{start_idx}+{len(keyword)}c"
                # Get the previous and next characters
                prev_char = self.query_input.get(f"{start_idx}-1c") if start_idx != "1.0" else ""
                next_char = self.query_input.get(end_idx)

                # Check if they are whitespace characters
                if (not prev_char or prev_char.isspace()) and (not next_char or next_char.isspace()):
                    self.query_input.tag_add("keyword", start_idx, end_idx)
                start_idx = end_idx
    
    """
    Constructor to instantiate the SQLInput class.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pack()
        self.query_input = ttk.Text(self, width=50, font=("Monaco", 12), wrap="word")
        self.query_input.pack(side = ttk.TOP, pady=4, padx = 8, fill="x", expand=True)
        self.query_input.tag_configure("keyword", foreground="#9090f5")

        self.execute_button = ttk.Button(self, text="Execute")
        self.execute_button.pack(side = ttk.BOTTOM, pady=4, padx = 8, anchor=ttk.S)
        self.execute_button.bind("<Button-1>", self.__execute_query)

        
        self.epsilon_input = Input(self, placeholder="Epsilon", default_value="1")
        self.epsilon_input.pack(side = ttk.RIGHT, pady=4, padx = 8)
        self.epsilon_label = ttk.Label(self, text="Epsilon", anchor=ttk.W)
        self.epsilon_label.pack(side = ttk.RIGHT, pady=4, padx = 8)

        # Change self.m

        self.query_input.bind("<KeyRelease>", self.highlight_keywords)

"""
Class LayoutHeader is a component that contains the input fields for the database connection.
"""
class LayoutHeader(ttk.Labelframe):
    """
    Method to handle the click event of the connect button.
    """
    def connect_button_click(self, event):
        self.connect_button.config(state="disabled")
        if self.master.inner_state.db_connection:
            # Enable input fields
            self.address_entry.entry.config(state="normal")
            self.database_entry.entry.config(state="normal")
            self.port_entry.entry.config(state="normal")
            self.user_entry.entry.config(state="normal")
            self.password_entry.entry.config(state="normal")

            self.master.disconnect()
        else:
            username = self.user_entry.entry.get()
            password = self.password_entry.entry.get()
            database = self.database_entry.entry.get()

            address = self.address_entry.entry.get()
            port = self.port_entry.entry.get()

            try:
                self.master.login(address, database, port, username, password)

                # Disable input fields
                self.address_entry.entry.config(state="disabled")
                self.database_entry.entry.config(state="disabled")
                self.port_entry.entry.config(state="disabled")
                self.user_entry.entry.config(state="disabled")
                self.password_entry.entry.config(state="disabled")
            except:  
                messagebox.showerror("Error", "Invalid username or password")

        self.refresh_connection_status()
        self.refresh_connect_button()
        self.master.refresh_content_layout()
        self.connect_button.config(state="normal")

    """
    Method to refresh the connection status.
    """
    def refresh_connection_status(self):
        if self.master.inner_state.db_connection:
            self.connected_label.config(text="Connected", style="success.TLabel")
        else:
            self.connected_label.config(text="Not Connected", style="danger.TLabel")

    """
    Method to refresh the connect button.
    """
    def refresh_connect_button(self):
        if self.master.inner_state.db_connection:
            self.connect_button.config(text="Disconnect", style="danger.TButton")
        else:
            self.connect_button.config(text="Connect", style="primary.TButton")

    """
    Method to instantiate the LayoutHeader class.
    """
    def __init__(self, *args, **kwargs):
        """
        self
        |-> inner_frame
            |-> address_entry
            |-> database_entry
            |-> port_entry
            |-> user_entry
            |-> password_entry
            |-> connect_button
            |-> connected_label
        """

        super().__init__(*args, **kwargs)
        self.pack()

        self.inner_frame = ttk.Frame(self)
        self.inner_frame.pack(pady = 8, padx = 8, fill="both")

        self.address_entry = InputWithLabel(self.inner_frame, placeholder="Address", label_text="Database Address")
        self.address_entry.pack(side = ttk.LEFT, padx = 8)

        self.database_entry = InputWithLabel(self.inner_frame, placeholder="Database", default_value="postgres", label_text="Database Name")
        self.database_entry.pack(side = ttk.LEFT, padx = 8)

        self.port_entry = InputWithLabel(self.inner_frame, placeholder="Port", label_text = "Database Port", default_value="5432")
        self.port_entry.pack(side = ttk.LEFT, padx = 8)

        self.user_entry = InputWithLabel(self.inner_frame, placeholder="User", default_value="postgres", label_text="Database User")
        self.user_entry.pack(side = ttk.LEFT, padx = 8)

        self.password_entry = InputWithLabel(self.inner_frame, show="*", placeholder="Password", label_text="Database Password")
        self.password_entry.pack(side = ttk.LEFT, padx = 8)

        self.connect_button = ttk.Button(self.inner_frame, text="Connect")
        self.connect_button.pack(side = ttk.LEFT, padx = 8, anchor=ttk.S)
        self.connect_button.bind("<Button-1>", self.connect_button_click)


        self.connected_label = ttk.Label(self.inner_frame)
        self.connected_label.pack(side = ttk.RIGHT, padx = 8, anchor=ttk.E)
        
        self.refresh_connection_status()

"""
Class LayoutContentNotLoggedIn is a component that contains the message to connect to the database first. Acts as a placeholder when there is no connection in place.
"""
class LayoutContentNotLoggedIn(ttk.LabelFrame):
    """
    Method to instantiate the LayoutContentNotLoggedIn class.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pack()
        self.label = ttk.Label(self, text="Please connect to the database first", anchor=ttk.CENTER)
        self.label.pack(side = ttk.TOP, fill="both", expand=True)

"""
Class LayoutContent is the main component that organizes the Query Explainer components.
"""
class LayoutContent(ttk.Frame):
    """
    Method to refresh the query content.
    """
    def refresh_query_content(self):
        # To be used after a new query
        image = Image.open('./assets/img/qep.png')
        image = image.resize((560, 560))
        self.graph_image = ImageTk.PhotoImage(image)
        self.graph_image_label.configure(image=self.graph_image)
        self.graph_image_label.image = self.graph_image

        # Refresh treeview explanation

    """
    Constructor to instantiate the LayoutContent class.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pack()
        """
        self
        |-> first_row
            |-> query_input_frame
                |-> query_input
            |-> query_result_frame
                |-> graph_image_label
        |-> second_row
            |-> query_table
            |-> query_explanation_frame
                |-> explanation_label
        
        """

        self.first_row = ttk.Frame(self, height=100)
        self.first_row.pack(side = ttk.TOP, fill="both", expand=True)

        self.query_input_frame = ttk.LabelFrame(self.first_row, borderwidth=2, text="SQL Input")
        self.query_input_frame.pack(side = ttk.LEFT, fill="both", pady=4, padx = (0,8), expand=True)
        self.sql_input = SQLInput(self.query_input_frame).pack(pady=4, padx = 8, fill="x")


        self.query_result_frame = ttk.LabelFrame(self.first_row, borderwidth=2, text="Physical Query Plan")
        self.query_result_frame.pack(side = ttk.LEFT, fill="both", pady=4, expand=True)

        self.graph_image = None
        self.graph_image_label = ttk.Label(self.query_result_frame, image=self.graph_image)
        self.graph_image_label.pack(side = ttk.TOP, pady=4, padx = 8, expand=True)

        self.second_row = ttk.Frame(self, height=100)
        self.second_row.pack(side = ttk.TOP, fill="both", expand=True)

        self.query_table_frame = ttk.Frame(self.second_row, borderwidth=2)
        self.query_table_frame.pack(side = ttk.LEFT, fill="both", expand=True)

        self.query_table = QueryTable(self.query_table_frame, width=560)
        self.query_table.pack(pady=4, padx = 8, side = ttk.LEFT, fill="both", expand=True)

        self.query_explanation_frame = ttk.LabelFrame(self.second_row, borderwidth=2, text="Query Explanation", width=560)
        self.query_explanation_frame.pack(side = ttk.LEFT, pady=4)

        self.query_explanation = QueryExplanation(self.query_explanation_frame)
        self.query_explanation.pack(pady=4, padx = 8)

"""
Class LayoutFooter is the footer component of QUPEX. 
"""
class LayoutFooter(ttk.Frame):
    """
    Constructor to instantiate the LayoutFooter class.
    """
    def __init__(self, *args, **kwargs):
        """
        self
        |-> label
        """

        super().__init__(*args, **kwargs)
        self.pack()

        self.label = ttk.Label(self, text="Made with <3 by Team 1", anchor=ttk.CENTER, foreground="grey")
        self.label.pack(side = ttk.TOP, fill="both", expand=True)

"""
Class InnerState is a class that contains the global variables of the application for the application logic.
"""
class InnerState:
    """
    Constructor to instantiate the InnerState class. 
    This is where to define the global variables of the application.
    """
    def __init__(self):
        self.db_connection = None
        self.graph = None

"""
Class App is the main component that organizes the QUPEX's components. 
This is the main component that is rendered in the mainloop.
"""
class App(ttk.Window):  
    """
    Constructor to instantiate the App class.
    """
    def __init__(self, inner_state: InnerState): 
        super().__init__(self, themename="darkly")

        self.title("QUPEX - Query Plan Explorer")
        self.iconbitmap("assets/img/icon.ico")
        self.icon_photo = ImageTk.PhotoImage(
            Image.open('assets/img/icon.ico')
        )
        self.iconphoto(True, self.icon_photo)
        self.geometry("1280x900")
        self.minsize(1024, 800)
        self.inner_state = inner_state
        self.generate_layout()
    
    """
    Method to refresh the content layout.
    """
    def refresh_content_layout(self):
        # Used to re-render the entirety of the content layout to its default state
        self.content.pack_forget()
        self.footer.pack_forget()
        if self.inner_state.db_connection:
            self.content = LayoutContent(self, borderwidth=2)
        else:
            self.content = LayoutContentNotLoggedIn(self, borderwidth=2)
        self.content.pack(side = ttk.TOP, padx=8, pady = 4, fill="both", expand=True)
        
        self.footer = LayoutFooter(self, borderwidth=2)
        self.footer.pack(side = ttk.TOP, padx=8, pady = 4, fill="x")

    """
    Method to login to the database.
    """
    def login(self, address, database, port, username, password):
        self.inner_state.db_connection = None
        db_connection = DB({
            "host": address, 
            "port": port, 
            "database": database,
            "user": username, 
            "password": password
        })
        self.inner_state.db_connection = db_connection
            
    """
    Method to disconnect from the database.
    """
    def disconnect(self):
        if self.inner_state.db_connection:
            self.inner_state.db_connection.close_connection()
            self.inner_state.db_connection = None
            self.refresh_content_layout()

    """
    Method to generate the layout of QUPEX.
    """
    def generate_layout(self):
        """
        self
        |-> header
        |-> content
        |-> footer
        """
        # Header that contains the input to the database connection
        self.header = LayoutHeader(self, borderwidth=2, text="Database Connection")
        self.header.pack(side = ttk.TOP, padx = 8, pady = 4, fill="x")

        # Content that contains the query input and the query result
        self.content = LayoutContentNotLoggedIn(self, borderwidth=2)
        self.content.pack(side = ttk.TOP, padx=8, pady = 4, fill="both", expand=True)

        # Footer that contains the credits
        self.footer = LayoutFooter(self, borderwidth=2)
        self.footer.pack(side = ttk.TOP, padx=8, pady = 4, fill="x")
      