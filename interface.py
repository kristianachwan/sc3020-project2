import ttkbootstrap as ttk
import tkinter as tk
from tkinter import messagebox
from explain import DB, Graph, GraphVisualizer, Node
import time

TEXT_PRIMARY_COLOR = "#F9F9F9"
TEXT_SECONDARY_COLOR = "grey"

### COMPONENTS ###
class Input(ttk.Entry):
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
        

    def foc_in(self, *args):
        if self.is_empty:
            self.delete("0", "end")
            self.configure(show=self.show)
            self.configure(foreground=self.color)

    def foc_out(self, *args):
        if not self.get():
            self.configure(show="")
            self.configure(foreground=self.placeholder_color)
            self.insert(0, self.placeholder)
            self.is_empty = True
        else:
            self.is_empty = False

class InputWithLabel(ttk.Frame):
    def __init__(self, master=None, label_text="", placeholder="", default_value=None, show="", **kwargs):
        super().__init__(master, **kwargs)
        self.pack()

        self.label = ttk.Label(self, text=label_text, anchor=ttk.W, foreground=TEXT_PRIMARY_COLOR)
        self.label.pack(side = ttk.TOP, fill="x")

        self.entry = Input(self, placeholder=placeholder, default_value=default_value, show=show, )
        self.entry.pack(side = ttk.TOP)         


### CONTENT SUBLAYOUTS ###
class QueryExplanation(ttk.Frame):

    def __recursive_update(self, node: Node, parent):
        def callback(event):
            self.selected_node = node
            self.query_explanation.config(text=node.cost_description)

        curnode = self.query_selection_tree.insert(parent, "end", text=node.node_type, values=(node.total_cost, node.row_count), tags=(node.node_type, node.uuid))
        self.query_selection_tree.tag_bind(node.uuid, "<<TreeviewSelect>>", callback=callback)
        for child in node.children:
            self.__recursive_update(child, curnode)

    def update_treeview(self, event):

        root: Node = self.master.master.master.master.inner_state.graph.root
        self.query_selection_tree.delete(*self.query_selection_tree.get_children())
        self.__recursive_update(root, "")


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

        self.query_selection_tree = ttk.Treeview(self.query_selection_frame, columns=("cost", "rows"), height=50)
        self.query_selection_tree.heading("#0", text="Query Plan")
        self.query_selection_tree.heading("#1", text="Cost")
        self.query_selection_tree.heading("#2", text="Rows")
        
        # Onclick event (based on tags)
        self.query_selection_tree.pack(side = ttk.LEFT, fill="y")

        self.query_explanation_frame = ttk.Frame(self)
        self.query_explanation_frame.pack(side = ttk.LEFT, fill="both", expand=True)

        explanation = "Lorem ipsum dolor sit amet, consectetur adipiscing elit"

        self.query_explanation = ttk.Label(self.query_explanation_frame, text=explanation, anchor=ttk.NW, width=100)
        self.query_explanation.pack(side = ttk.LEFT)

class QueryTable(ttk.Frame):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pack()

        self.notebook = ttk.Notebook(self)
        self.notebook.pack(side = ttk.TOP, fill="both", expand=True)

        self.query_table = ttk.Frame(self.notebook, width=480, height=480)
        self.query_table.pack(fill="y")

        self.schema_table = ttk.Frame(self.notebook, width=480, height=480)
        self.schema_table.pack(fill="y")

        self.notebook.add(self.query_table, text="Query Statistics")
        self.notebook.add(self.schema_table, text="Schemas")
        """
        self
        |-> query_table (several entries)
        """

class SQLInput(ttk.Frame):
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
        
        #try:
        graph = Graph(query_plan, self.master.master.master.master.inner_state.db_connection)
        self.master.master.master.master.inner_state.graph = graph

        graphviz = GraphVisualizer(graph)

        self.master.master.master.refresh_query_content()
        #except Exception as e:
        #    messagebox.showerror("Error", str(e))
        #    reset_connection()
            
        self.master.master.master.query_explanation.update_treeview(None)
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pack()
        self.query_input = ttk.Text(self, width=50, font=("Consolas", 12), wrap="word")
        self.query_input.pack(side = ttk.TOP, pady=4, padx = 8, fill="x", expand=True)

        self.execute_button = ttk.Button(self, text="Execute")
        self.execute_button.pack(side = ttk.BOTTOM, pady=4, padx = 8, anchor=ttk.S)
        self.execute_button.bind("<Button-1>", self.__execute_query)

### LAYOUT ###
class LayoutHeader(ttk.Labelframe):

    def connect_button_click(self, event):
        username = self.user_entry.entry.get()
        password = self.password_entry.entry.get()

        address = self.address_entry.entry.get()
        port = self.port_entry.entry.get()

        self.master.login(address, port, username, password)

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)
        self.pack()

        self.inner_frame = ttk.Frame(self)
        self.inner_frame.pack(pady = 8, padx = 8, fill="both")

        self.address_entry = InputWithLabel(self.inner_frame, placeholder="Address", label_text="Database Address", default_value="0.tcp.ap.ngrok.io")
        self.address_entry.pack(side = ttk.LEFT, padx = 8)

        self.port_entry = InputWithLabel(self.inner_frame, placeholder="Port", label_text = "Database Port", default_value="15062")
        self.port_entry.pack(side = ttk.LEFT, padx = 8)

        self.user_entry = InputWithLabel(self.inner_frame, placeholder="User", default_value="postgres", label_text="Database User")
        self.user_entry.pack(side = ttk.LEFT, padx = 8)

        self.password_entry = InputWithLabel(self.inner_frame, show="*", placeholder="Password", label_text="Database Password", default_value="sc3020ggez")
        self.password_entry.pack(side = ttk.LEFT, padx = 8)

        self.connect_button = ttk.Button(self.inner_frame, text="Connect")
        self.connect_button.pack(side = ttk.LEFT, padx = 8, anchor=ttk.S)
        self.connect_button.bind("<Button-1>", self.connect_button_click)

class LayoutContentNotLoggedIn(ttk.LabelFrame):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pack()
        self.label = ttk.Label(self, text="Please connect to the database first", anchor=ttk.CENTER)
        self.label.pack(side = ttk.TOP, fill="both", expand=True)

class LayoutContent(ttk.Frame):
    def refresh_query_content(self):
        # To be used after a new query
        self.graph_image = ttk.PhotoImage(file="./qep.png")
        self.graph_image_label.configure(image=self.graph_image)
        self.graph_image_label.image = self.graph_image

        # Refresh treeview explanation

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


        self.query_table = QueryTable(self.second_row)
        self.query_table.pack(pady=4, padx = 8, fill="x", side = ttk.LEFT, expand=True)

        self.query_explanation_frame = ttk.LabelFrame(self.second_row, borderwidth=2, text="Query Explanation")
        self.query_explanation_frame.pack(side = ttk.LEFT, fill="both", pady=4, expand=True)

        self.query_explanation = QueryExplanation(self.query_explanation_frame)
        self.query_explanation.pack(pady=4, padx = 8, fill="x")

### APPLICATION LOGIC ###
class InnerState:
    # Define the global variables here
    def __init__(self):
        self.db_connection = None
        self.graph = None

class App(ttk.Window):  
    def __init__(self, inner_state: InnerState): 
        super().__init__(self, themename="darkly")
        
        self.title("SC3020 Project 2")
        self.geometry("1280x900")
        self.minsize(1024, 800)
        self.inner_state = inner_state
        self.generate_layout()
    
    def refresh_content_layout(self):
        # Used to re-render the entirety of the content layout to its default state
        self.content.pack_forget()
        if self.inner_state.db_connection is not None:
            self.content = LayoutContent(self, borderwidth=2)
        else:
            self.content = LayoutContentNotLoggedIn(self, borderwidth=2, text="Content")
        self.content.pack(side = ttk.TOP, padx=8, pady = 4, fill="both", expand=True)


    def login(self, address, port, username, password):
        self.db_connection = None
        try:
            db_connection = DB({
                "host": address, 
                "port": port, 
                "database": "postgres", # hard coded 
                "user": username, 
                "password": password
            })
            self.inner_state.db_connection = db_connection
            self.refresh_content_layout()
        except:
            self.refresh_content_layout()
            messagebox.showerror("Error", "Invalid username or password")
        
    def generate_layout(self):
        # Header that contains the input to the database connection
        self.header = LayoutHeader(self, borderwidth=2, text="Database Connection")
        self.header.pack(side = ttk.TOP, padx = 8, pady = 4, fill="x")

        # Content that contains the query input and the query result
        self.content = LayoutContentNotLoggedIn(self, borderwidth=2)
        self.content.pack(side = ttk.TOP, padx=8, pady = 4, fill="both", expand=True)
      