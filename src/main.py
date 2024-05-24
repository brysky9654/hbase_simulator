import os
import re
import shutil
import time as t
import json
import tkinter as tk
from tkinter import ttk

from models.table import *

TABLES = []

def load_table(table_name: str) -> Table:
    # from a json like this {"enabled": true, "name": "tabla", "column_families": {"c1": {"version": "1"}, "c2": {"version": "1"}}}

    json_path = f"tables/{table_name}/config.json"

    with open(json_path, "r") as f:
        data = json.load(f)
        table = Table(name=data['name'], column_families=data['column_families'])
        table.enabled = data['enabled']
        
    return table

def list_tables():
    tables = os.listdir("tables")

    for table in tables:
        print(table)

def init() -> None:
    global TABLES

    if not os.path.exists("tables"):
        os.makedirs("tables")

    table_names = os.listdir("tables")
    tables = []

    for table in table_names:
        tables.append(load_table(table))

    TABLES = tables

def valid_string(s):
    return True

def create_table(table_n, columns, flag):
    global TABLES
    table = Table(table_n)
    table.column_families = {col: '1' for col in columns}
    TABLES.append(table)

def list_tables():
    return [table.name for table in TABLES]

def execute_command(command):
    spl = command.split(" ")
    if spl[0] == "create":
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

    elif spl[0] == "list":
        tables = list_tables()
        if tables:
            return "\n".join(tables)
        return "No tables found"

    elif spl[0] == "disable":
        table_name = spl[1]
        if not valid_string(table_name):
            return "Invalid table name"
        table_name = table_name.replace("'", "")
        table = next((table for table in TABLES if table.name == table_name), None)
        if not table:
            return f"{table_name} table not found"
        table.disable()
        return f"Table {table_name} disabled"

    elif spl[0] == "enable":
        table_name = spl[1]
        if not valid_string(table_name):
            return "Invalid table name"
        table_name = table_name.replace("'", "")
        table = next((table for table in TABLES if table.name == table_name), None)
        if not table:
            return f"{table_name} table not found"
        table.enable()
        return f"Table {table_name} enabled"

    elif spl[0] == "is_disabled":
        table_name = spl[1]
        if not valid_string(table_name):
            return "Invalid table name"
        table_name = table_name.replace("'", "")
        table = next((table for table in TABLES if table.name == table_name), None)
        if not table:
            return f"{table_name} table not found"
        return str(not table.enabled)

    elif spl[0] == "alter":
        table_name = spl[1]
        column_family = spl[2]
        version = spl[3]
        if not valid_string(table_name):
            return "Invalid table name"
        if not valid_string(column_family):
            return "Invalid column family name"
        if not valid_string(version):
            return "Invalid version"
        table_name = table_name.replace("'", "")
        column_family = column_family.replace("'", "")
        version = version.replace("'", "")
        table = next((table for table in TABLES if table.name == table_name), None)
        if not table:
            return f"{table_name} table not found"
        table.alter(column_family, version)
        table.write_to_memory()
        return f"Table {table_name} altered"

    elif spl[0] == "drop":
        table_name = spl[1]
        if not valid_string(table_name):
            return "Invalid table name"
        table_name = table_name.replace("'", "")
        table = next((table for table in TABLES if table.name == table_name), None)
        if not table:
            return f"{table_name} table not found"
        table.drop()
        return f"Table {table_name} dropped"

    elif spl[0] == "drop_all":
        regex = spl[1]
        if not valid_string(regex):
            return "Invalid regex"
        regex = regex.replace("'", "")
        pattern = re.compile(regex)
        table_names = os.listdir("tables")
        dropped_tables = []
        for table_name in table_names:
            if pattern.match(table_name):
                table_path = os.path.join("tables", table_name)
                if os.path.isdir(table_path):
                    shutil.rmtree(table_path)
                else:
                    os.remove(table_path)
                dropped_tables.append(table_name)
        return f"{', '.join(dropped_tables)} table(s) dropped"

    elif spl[0] == "describe":
        table_name = spl[1]
        if not valid_string(table_name):
            return "Invalid table name"
        table_name = table_name.replace("'", "")
        table = next((table for table in TABLES if table.name == table_name), None)
        if not table:
            return f"{table_name} table not found"
        return table.describe()

    elif spl[0] == "put":
        table_name = spl[1]
        row_id = spl[2]
        column = spl[3]
        value = spl[4]
        if not valid_string(table_name):
            return "Invalid table name"
        if not valid_string(row_id):
            return "Invalid row id"
        if not valid_string(column):
            return "Invalid column"
        if not valid_string(value):
            if value.find(".") != -1:
                try:
                    value = float(value)
                except:
                    value = value.replace("'", "")
            elif value.isdigit():
                value = int(value)
            else:
                value = value.replace("'", "")
        else:
            value = value.replace("'", "")
        column_family = column.split(":")[0]
        column_q = column.split(":")[1]
        table_name = table_name.replace("'", "")
        row_id = row_id.replace("'", "")
        column_family = column_family.replace("'", "")
        column_q = column_q.replace("'", "")
        table = next((table for table in TABLES if table.name == table_name), None)
        if column_family not in table.column_families:
            return "Column family not found"
        if not table:
            return f"{table_name} table not found"
        hf = HFile(table=table.name)
        hf.put(row_id, column_family, column_q, value)
        del hf
        return f"Inserted {value} into {table_name}"

    elif spl[0] == "get":
        table_name = spl[1]
        row_id = spl[2]
        if not valid_string(table_name):
            return "Invalid table name"
        if not valid_string(row_id):
            return "Invalid row id"
        table_name = table_name.replace("'", "")
        row_id = row_id.replace("'", "")
        if len(spl) > 3 and spl[3] == "{COLUMN":
            if not valid_string(spl[5][:-1]):
                return "Invalid column"
            column_f = spl[5].split(":")[0].replace("'", "")
            column_q = spl[5].split(":")[1].replace("'", "").rstrip("}")
        else:
            column_f = None
            column_q = None
        table = next((table for table in TABLES if table.name == table_name), None)
        if not table:
            return "Table not found"
        hf = HFile(table=table.name)
        row_data = hf.get(row_id)
        output = f"{'COLUMN'.ljust(40)} CELL\n"
        if column_f is None and column_q is None:
            for column_family, columns in row_data.items():
                for column, value in columns.items():
                    keys = list(value.keys())
                    first = max(keys)
                    s = f"{column_family}:{column}"
                    output += f"{s.ljust(40)} timestamp={first},value={value[first]}\n"
        else:
            for column_family, columns in row_data.items():
                for column, value in columns.items():
                    if column_f == column_family and column_q == column:
                        keys = list(value.keys())
                        first = max(keys)
                        s = f"{column_family}:{column}"
                        output += f"{s.ljust(40)} timestamp={first},value={value[first]}\n"
        del hf
        return output

    elif spl[0] == "scan":
        table_name = spl[1]
        if not valid_string(table_name):
            return "Invalid table name"
        table_name = table_name.replace("'", "")
        table = next((table for table in TABLES if table.name == table_name), None)
        if not table:
            return f"{table_name} table not found"
        hf = HFile(table=table.name)
        rows = hf.scan()
        output = f"{'ROW'.ljust(40)} COLUMN+CELL\n"
        for row, columns in rows.items():
            for column_family, columns in columns.items():
                for column, value in columns.items():
                    keys = list(value.keys())
                    first = max(keys)
                    s = f"{row}"
                    output += f"{s.ljust(40)} column={column_family}:{column}, timestamp={first}, value={value[first]}\n"
        del hf
        return output

    elif spl[0] == "delete":
        table_name = spl[1]
        row_id = spl[2]
        column = spl[3]
        timestamp = spl[4]
        if not valid_string(table_name):
            return "Invalid table name"
        if not valid_string(row_id):
            return "Invalid row id"
        if not valid_string(column):
            return "Invalid column"
        column_family = column.split(":")[0]
        column_q = column.split(":")[1]
        table_name = table_name.replace("'", "")
        row_id = row_id.replace("'", "")
        column_family = column_family.replace("'", "")
        column_q = column_q.replace("'", "")
        table = next((table for table in TABLES if table.name == table_name), None)
        if not table:
            return f"{table_name} table not found"
        hf = HFile(table=table.name)
        if row_id in hf.rows and column_family in hf.rows[row_id] and column_q in hf.rows[row_id][column_family]:
            if timestamp in hf.rows[row_id][column_family][column_q]:
                hf.delete(row_id, column_family, column_q, timestamp)
        del hf
        return f"Deleted column {column} from row {row_id} in table {table_name}"

    elif spl[0] == "delete_all":
        table_name = spl[1]
        row_id = spl[2]
        if not valid_string(table_name):
            return "Invalid table name"
        if not valid_string(row_id):
            return "Invalid row id"
        table_name = table_name.replace("'", "")
        row_id = row_id.replace("'", "")
        table = next((table for table in TABLES if table.name == table_name), None)
        if not table:
            return f"{table_name} table not found"
        hf = HFile(table=table.name)
        if row_id in hf.rows:
            hf.delete_all(row_id)
        del hf
        return f"Deleted all columns from row {row_id} in table {table_name}"

    elif spl[0] == "count":
        table_name = spl[1]
        if not valid_string(table_name):
            return "Invalid table name"
        table_name = table_name.replace("'", "")
        table = next((table for table in TABLES if table.name == table_name), None)
        if table:
            data_path = os.path.join("tables", table_name, "regions", "region1", "hfile.json")
            if os.path.exists(data_path):
                with open(data_path, "r") as f:
                    data = json.load(f)
                    rows_n = len(data)
                    return f"{rows_n} row(s) found in table {table_name}."
            else:
                return "Table is empty."
        else:
            return f"{table_name} table not found."

    elif spl[0] == "truncate":
        table_name = spl[1]
        if not valid_string(table_name):
            return "Invalid table name"
        table_name = table_name.replace("'", "")
        table = next((table for table in TABLES if table.name == table_name), None)
        if not table:
            return f"{table_name} table not found"
        fams = list(table.column_families.keys())
        table.disable()
        t.sleep(40)
        table.drop()
        create_table(table_name, fams, False)
        return f"Truncated table {table_name}"

    elif spl[0] == "insert_many":
        table_name = spl[1]
        inserts = spl[2:]
        for ins in inserts:
            if not ins.startswith("{") or not ins.endswith("}"):
                return "Invalid insert, missing '{' or '}'. Insert Many does not support spaces inside values."
        if not valid_string(table_name):
            return "Invalid table name"
        table_name = table_name.replace("'", "")
        table = next((table for table in TABLES if table.name == table_name), None)
        if not table:
            return f"{table_name} table not found"
        hf = HFile(table=table.name)
        for insert in inserts:
            row_id = insert.split(",")[0].replace("{", "").replace("'", "")
            column = insert.split(",")[1].replace("'", "")
            value = insert.split(",")[2].replace("}", "")
            if value.isdigit():
                value = int(value)
            elif value.find(".") != -1:
                try:
                    value = float(value)
                except:
                    value = value
            else:
                if valid_string(value):
                    value = value.replace("'", "")
                else:
                    continue
            column_family = column.split(":")[0]
            column_q = column.split(":")[1]
            hf.put(row_id, column_family, column_q, value)
        del hf
        return f"Inserted many into {table_name}"

    elif spl[0] == "update_many":
        table_name = spl[1]
        column = spl[2]
        value = spl[3]
        rows = spl[4:]
        if not valid_string(table_name):
            return "Invalid table name"
        if not valid_string(column):
            return "Invalid column"
        if not valid_string(value):
            if value.isdigit():
                value = int(value)
            elif value.find(".") != -1:
                try:
                    value = float(value)
                except:
                    value = value
        else:
            value = value.replace("'", "")
        for r in rows:
            if not valid_string(r):
                continue
        table_name = table_name.replace("'", "")
        column = column.replace("'", "")
        column_family = column.split(":")[0]
        column_q = column.split(":")[1]
        rows = [r.replace("'", "") for r in rows]
        table = next((table for table in TABLES if table.name == table_name), None)
        if not table:
            return f"{table_name} table not found"
        hf = HFile(table=table.name)
        for row in rows:
            hf.put(row, column_family, column_q, value)
        del hf
        return f"Updated many in {table_name}"

    else:
        return "Unrecognized command"

def main():
    global TABLES
    init()
    for table in TABLES:
        print(table.name)
    input_command = ""
    while input_command != "exit":
        input_command = input("habase(main)> ")
        if input_command == "exit":
            exit()
        output = execute_command(input_command)
        if output:
            print(output)

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