import ttkbootstrap as ttk
import tkinter as tk
from tkinter import messagebox
import time

TEXT_PRIMARY_COLOR = "#F9F9F9"
TEXT_SECONDARY_COLOR = "grey"

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
            

class QueryExplanation(ttk.Frame):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pack(expand=True, fill="both")
        """
        self
        |-> query_selection_frame
            |-> query_selection (treeview)
        |-> query_explanation_frame
            |-> query_explanation (label)
        """

        self.query_selection_frame = ttk.Frame(self, width=5)
        self.query_selection_frame.pack(side = ttk.LEFT, fill="y", padx=(0, 16))

        self.query_selection_tree = ttk.Treeview(self.query_selection_frame, columns=("cost"), height=50)
        self.query_selection_tree.heading("#0", text="Query Plan")
        self.query_selection_tree.heading("#1", text="Cost")
        self.query_selection_tree.column("#0", width=125)
        self.query_selection_tree.column("#1", width=50)

        # Example data
        node1 = self.query_selection_tree.insert("", "end", text="Index Scan", values=("100"), tags=("index_scan"))
        node2 = self.query_selection_tree.insert("", "end", text="Seq Scan", values=("200"), tags=("seq_scan"))
        node3 = self.query_selection_tree.insert(node1, "end", text="Hash Join", values=("200"), tags=("hash_join"))
        
        # Onclick event (based on tags)
        self.query_selection_tree.tag_bind("index_scan", "<<TreeviewSelect>>", callback = lambda event: self.query_explanation.config(text="Index Scan Explanation"))
        self.query_selection_tree.tag_bind("seq_scan", "<<TreeviewSelect>>", callback = lambda event: self.query_explanation.config(text="Seq Scan Explanation"))
        self.query_selection_tree.tag_bind("hash_join", "<<TreeviewSelect>>", callback = lambda event: self.query_explanation.config(text="Hash Join Explanation"))

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
        """
        self
        |-> query_table (several entries)
        """

class SQLInput(ttk.Frame):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pack()
        self.query_input = ttk.Text(self, width=50, font=("Consolas", 12), wrap="word")
        self.query_input.pack(side = ttk.TOP, pady=4, padx = 8, fill="x", expand=True)

        self.execute_button = ttk.Button(self, text="Execute")
        self.execute_button.pack(side = ttk.BOTTOM, pady=4, padx = 8, anchor=ttk.S)


class InnerState:
    # Define the global variables here
    def __init__(self):
        self.logged_in = False


class App(ttk.Window):  
    def __init__(self, inner_state: InnerState): 
        super().__init__(self, themename="darkly")
        
        self.title("SC3020 Project 2")
        self.geometry("1280x900")
        self.minsize(1024, 800)
        self.inner_state = inner_state
        self.generate_layout()
    
    def __refresh_content_layout(self):
        self.content.pack_forget()
        if self.inner_state.logged_in:
            self.content = LayoutContent(self, borderwidth=2)
        else:
            self.content = LayoutContentNotLoggedIn(self, borderwidth=2, text="Content")
        self.content.pack(side = ttk.TOP, padx=8, pady = 4, fill="both", expand=True)

    def login(self, address, port, username, password):
        # testing
        if username == "postgres" and password == "postgres":
            self.inner_state.logged_in = True
            self.__refresh_content_layout()
        else:
            self.inner_state.logged_in = False
            self.__refresh_content_layout()
            messagebox.showerror("Error", "Invalid username or password")
        
        
        
    def generate_layout(self):
        # Header that contains the input to the database connection
        self.header = LayoutHeader(self, borderwidth=2, text="Database Connection")
        self.header.pack(side = ttk.TOP, padx = 8, pady = 4, fill="x")

        # Content that contains the query input and the query result
        self.content = LayoutContentNotLoggedIn(self, borderwidth=2)
        self.content.pack(side = ttk.TOP, padx=8, pady = 4, fill="both", expand=True)
        

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

        self.address_entry = InputWithLabel(self.inner_frame, placeholder="Address", label_text="Database Address")
        self.address_entry.pack(side = ttk.LEFT, padx = 8)

        self.port_entry = InputWithLabel(self.inner_frame, placeholder="Port", label_text = "Database Port", default_value="5432")
        self.port_entry.pack(side = ttk.LEFT, padx = 8)

        self.user_entry = InputWithLabel(self.inner_frame, placeholder="User", default_value="postgres", label_text="Database User")
        self.user_entry.pack(side = ttk.LEFT, padx = 8)

        self.password_entry = InputWithLabel(self.inner_frame, show="*", placeholder="Password", label_text="Database Password")
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
            |-> query_table_frame
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

        self.graph_image = ttk.PhotoImage(file="./download.png")
        self.graph_image_label = ttk.Label(self.query_result_frame, image=self.graph_image)
        self.graph_image_label.pack(side = ttk.TOP, pady=4, padx = 8, expand=True)

        self.second_row = ttk.Frame(self, height=100)
        self.second_row.pack(side = ttk.TOP, fill="both", expand=True)

        self.query_table_frame = ttk.LabelFrame(self.second_row, borderwidth=2, text="Query Table")
        self.query_table_frame.pack(side = ttk.LEFT, fill="both", pady=4, padx = (0,8), expand=True)

        self.query_explanation_frame = ttk.LabelFrame(self.second_row, borderwidth=2, text="Query Explanation")
        self.query_explanation_frame.pack(side = ttk.LEFT, fill="both", pady=4, expand=True)

        self.query_explanation = QueryExplanation(self.query_explanation_frame)
        self.query_explanation.pack(pady=4, padx = 8, fill="x")

