from models.table import Table, HFile
import shutil
import json
import os
import re
import time as t
import tkinter as tk
from tkinter import ttk

TABLES = []

def load_table(table_name: str) -> Table:
    try:
        json_path = f"tables/{table_name}/config.json"
        with open(json_path, "r") as f:
            data = json.load(f)
            table = Table(name=data['name'], column_families=data['column_families'])
            table.enabled = data['enabled']
        return table
    except Exception as e:
        print(f"Error loading table {table_name}: {e}")
        return None

def init() -> None:
    global TABLES
    if not os.path.exists("tables"):
        os.makedirs("tables")
    table_names = os.listdir("tables")
    tables = [load_table(table) for table in table_names if load_table(table) is not None]
    TABLES = tables

def list_tables() -> str:
    tables = os.listdir("tables")
    return "\n".join(tables)

def create_table(table_name: str, columns: list[str], printdata:bool) -> None:
    global TABLES
    table = Table(name=table_name, column_families={column: {"version": "1"} for column in columns})
    if printdata:
        print(f"=> Hbase:: Table - {table_name}")
    table.write_to_memory()
    TABLES.append(table)
    
def valid_string(s: str) -> bool:
    return s[0] == "'" and s[-1] == "'"

def execute_command(command: str) -> str:
    global TABLES
    init()
    output = []

    for table in TABLES:
        output.append(table.name)

    spl = command.split(" ")

    try:
        match spl[0]:
            case "create":
                table_n = spl[1]
                columns = spl[2:]
                if not valid_string(table_n):
                    return "Invalid table name"
                table_n = table_n.replace("'", "")
                if any([not valid_string(column) for column in columns]):
                    return "Invalid column family name"
                columns = [column.replace("'", "") for column in columns]
                create_table(table_n, columns, True)
                return f"Table {table_n} created with columns {', '.join(columns)}"
            
            case "list":
                return list_tables()
            
            case "scan":
                table_name = spl[1]
                if not valid_string(table_name):
                    return "Invalid table name"
                table_name = table_name.replace("'", "")
                table = next((table for table in TABLES if table.name == table_name), None)
                if table is None:
                    return f"Table {table_name} not found"
                hf = HFile(table=table.name)
                rows = hf.scan()
                result = []
                result.append(f"{'ROW'.ljust(40)} COLUMN+CELL")
                for row, columns in rows.items():
                    for column_family, columns in columns.items():
                        for column, value in columns.items():
                            keys = list(value.keys())
                            first = max(keys)
                            s = f"{row}"
                            result.append(f"{s.ljust(40)} column={column_family}:{column}, timestamp={first}, value={value[first]}")
                del hf
                return "\n".join(result)
            
            case _:
                return "Unrecognized command"
    except Exception as e:
        return f"Error: {str(e)}"

def main():
    while True:
        command = input("habase(main)> ")
        if command == "exit":
            break
        print(execute_command(command))

def run_gui():
    def on_submit():
        command = command_entry.get()
        output = execute_command(command)
        result_text.config(state=tk.NORMAL)
        result_text.insert(tk.END, f"habase(main)> {command}\n{output}\n")
        result_text.config(state=tk.DISABLED)
        command_entry.delete(0, tk.END)

    init()

    root = tk.Tk()
    root.title("HBase GUI")

    main_frame = ttk.Frame(root, padding="10")
    main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

    command_label = ttk.Label(main_frame, text="Enter Command:")
    command_label.grid(row=0, column=0, sticky=tk.W)

    command_entry = ttk.Entry(main_frame, width=50)
    command_entry.grid(row=0, column=1, sticky=(tk.W, tk.E))
    command_entry.focus()

    submit_button = ttk.Button(main_frame, text="Submit", command=on_submit)
    submit_button.grid(row=0, column=2, sticky=tk.W)

    result_text = tk.Text(main_frame, width=80, height=20, state=tk.DISABLED)
    result_text.grid(row=1, column=0, columnspan=3, sticky=(tk.W, tk.E, tk.N, tk.S))

    root.mainloop()

if __name__ == "__main__":
    run_gui()