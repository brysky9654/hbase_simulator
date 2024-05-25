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
        return f"Error loading table {table_name}: {e}"

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

def create_table(table_name: str, columns: list[str], printdata:bool) -> str:
    global TABLES
    message = ""
    table = Table(name=table_name, column_families={column: {"version": "1"} for column in columns})
    
    if printdata:
        message = (f"=> Hbase:: Table - {table_name}")
    
    table.write_to_memory()
    TABLES.append(table)
    
    return message
    
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
                # create 'table name', 'column family', 'column family', ...
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
                # list
                return list_tables()
            
            case "scan":
                # scan 'table name'
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
                            
                            if len(keys) == 0:
                                continue
                            
                            first = max(keys)
                            
                            s = f"{row}"
                            
                            result.append(f"{s.ljust(40)} column={column_family}:{column}, timestamp={first}, value={value[first]}")
                del hf
                return "\n".join(result)
            
            case "disable":
                # disable 'table name'
                table_name = spl[1]
                if not valid_string(table_name):
                    return "Invalid table name"
                table_name = table_name.replace("'", "")
                table = next((table for table in TABLES if table.name == table_name), None)
                if not table:
                    return f"{table_name} table not found"
                table.disable()
                return f"Table {table_name} disabled"

            case "enable":
                # enable 'table name'
                table_name = spl[1]
                if not valid_string(table_name):
                    return "Invalid table name"
                
                table_name = table_name.replace("'", "")
                table = next((table for table in TABLES if table.name == table_name), None)
                if not table:
                    return f"{table_name} table not found"
                table.enable()
                return f"Table {table_name} enabled"

            case "is_disabled":
                # is_disabled 'table name'
                table_name = spl[1]
                if not valid_string(table_name):
                    return "Invalid table name"
                table_name = table_name.replace("'", "")
                table = next((table for table in TABLES if table.name == table_name), None)
                if not table:
                    return f"{table_name} table not found"
                return str(not table.enabled)
            
            case "alter":
                # > alter 'table name' 'column family' 'version'
                # > alter 'table name' 'delete' 'column family'
                # > alter 'table name' 'add' 'column family' 'version'
                
                if len(spl) == 4 and spl[2] != "'delete'":
                    # Alter: Modificar las versiones de un column family
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
                    if table.enabled:
                        return f"ERROR: Table {table_name} is enabled. Disable it first."
                    
                    table.alter(column_family, version)
                    table.write_to_memory()

                    hf = HFile(table=table.name)
                    hf.update_changes(table)

                    del hf
                    
                    return f"Table {table_name} altered with column family {column_family} and version {version}"

                elif len(spl) == 4 and spl[2] == "'delete'":
                    # Alter: Eliminar un column family
                    table_name = spl[1]
                    column_family = spl[3]
                    
                    if not valid_string(table_name):
                        return "Invalid table name"
                    if not valid_string(column_family):
                        return "Invalid column family name"
                    
                    table_name = table_name.replace("'", "")
                    column_family = column_family.replace("'", "")
                    table = next((table for table in TABLES if table.name == table_name), None)
                    
                    if not table:
                        return f"{table_name} table not found"
                    if table.enabled:
                        return f"ERROR: Table {table_name} is enabled. Disable it first."
                    
                    table.drop_column_family(column_family)
                    table.write_to_memory()

                    hf = HFile(table=table.name)
                    hf.update_changes(table)

                    del hf
                    
                    return f"Column family {column_family} deleted from table {table_name}"

                elif len(spl) == 5 and spl[2] == "'add'":
                    # Alter: Agregar un column family
                    table_name = spl[1]
                    column_family = spl[3]
                    version = spl[4]
                    
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
                    if table.enabled:
                        return f"ERROR: Table {table_name} is enabled. Disable it first."
                    
                    table.add_column_family(column_family, version)
                    table.write_to_memory()

                    hf = HFile(table=table.name)
                    hf.update_changes(table)

                    del hf
                    
                    return f"Column family {column_family} added to table {table_name} with version {version}"
                
                else:
                    return "Invalid alter command. 'alter' command can add a column family, delete a column family or alter a column family version."
            
            case "drop":
                # drop ’<table name>’
                table_name = spl[1]
                
                if not valid_string(table_name):
                    return "Invalid table name"
                    
                if not table:
                    return f"{table_name} table not found"

                table_name = table_name.replace("'", "")

                table = next((table for table in TABLES if table.name == table_name), None)
                
                if table.enabled:
                    print(f"ERROR: Table {table_name} is enabled. Disable it first.")
                else:
                    table.drop()
                    return f"{table_name} table dropped"
            
            case "drop_all":
                # drop_all ’<regex>’
                regex = spl[1]
                if not valid_string(regex):
                    return "Invalid regex"
                
                regex = regex.replace("'", "")

                pattern = re.compile(regex)

                table_names = os.listdir("tables")

                message = ""
                for table_name in table_names:
                    if pattern.match(table_name):
                        table_path = os.path.join("tables", table_name)
                        if os.path.isdir(table_path): 
                            shutil.rmtree(table_path)
                            message += f"{table_name} table(s) dropped\n"
                        else:
                            os.remove(table_path)
                            message += f"{table_name} table(s) dropped\n"

                return message

            case "describe":
                # describe ’<table name>’
                table_name = spl[1]

                if not valid_string(table_name):    
                    return "Invalid table name"

                if not table:
                    return f"{table_name} table not found"

                table_name = table_name.replace("'", "")

                table = next((table for table in TABLES if table.name == table_name), None)

                return table.describe()
            
            # --- DML Functions ---
            case "put":
                # put ‘table name’, ’row ’, 'colfamily:colname', ’new value’
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

                if not table.enabled:
                    return f"ERROR: Table {table_name} is disabled. Enable it first."
                    
                else:
                    hf.put(row_id, column_family, column_q, value)

                del hf
                return f"Row {row_id} inserted in table {table_name}.\n"

            case "get":
                # get ’<table name>’, ’row id’
                """
                COLUMN                        CELL
                details:name                  timestamp=123456789, value=John
                details:age                   timestamp=123456789, value=30
                """
                table_name = spl[1]
                row_id = spl[2]

                if not valid_string(table_name):
                    return "Invalid table name"
                    

                if not valid_string(row_id):
                    return "Invalid row id"
                    

                table_name = table_name.replace("'", "")
                row_id = row_id.replace("'", "")
                
                if len(spl) > 3:
                    """
                    COLUMN                              CELL
                    personal_data:pet                   timestamp=123456789, value=dog
                    """
                    #spl ['get', 'tabla4', '1', '{COLUMN', '=>', 'fam:cq}']
                    # get ’<table name>’, ’row id’, {COLUMN => 'cfamily:columnq'}

                    if spl[3] == "{COLUMN":
                        if not valid_string(spl[5][:-1]):
                            return "Invalid column"
                            
                        
                        column_f = spl[5].split(":")[0].replace("'", "")
                        column_q = spl[5].split(":")[1].replace("'", "")
                        column_q = column_q[:-1]  # Eliminar el último caracter que es el '}'

                    else:
                        return "Invalid command"

                else:
                    # get '<table name>', 'row id'
                    column_f = None
                    column_q = None

                table = next((table for table in TABLES if table.name == table_name), None)
                
                if table is not None:
                    hf = HFile(table=table.name)
                    row_data = hf.get(row_id)
                    
                    string = f"{'COLUMN'.ljust(40)} CELL\n"
                    
                    if column_f is None and column_q is None:
                        for column_family, columns in row_data.items():
                            for column, value in columns.items():
                                keys = list(value.keys())

                                if len(keys) == 0:
                                    continue

                                first = max(keys)
                                s = f"{column_family}:{column}"
                                string += f"{s.ljust(40)} timestamp={first},value={value[first]}\n"

                    else:
                        for column_family, columns in row_data.items():
                            for column, value in columns.items():
                                if column_f == column_family and column_q == column:
                                    keys = list(value.keys())

                                    if len(keys) == 0:
                                        continue

                                    first = max(keys)
                                    s = f"{column_family}:{column}"
                                    string += f"{s.ljust(40)} timestamp={first},value={value[first]}\n"
                else:
                    string = "Table not found"
                    

                del hf
                return string
                
            case "delete":
                # delete '<table name>', 'row id', 'column family:column qualifier', timestamp

                if len(spl) < 5:
                    return "Invalid command. Usage: delete '<table name>', 'row id', 'column family:column qualifier', timestamp"

                tableName = spl[1]
                rowId = spl[2]
                column = spl[3]
                timestamp = spl[4]
                
                if not table:
                    return f"{tableName} table not found"
                    

                if not valid_string(tableName):
                    return "Invalid table name"
                    

                if not valid_string(rowId):
                    return "Invalid row id"
                    

                if not valid_string(column):
                    return("Invalid column")
                    
                columnFamily = column.split(":")[0]
                columnQualifier = column.split(":")[1]

                tableName = tableName.replace("'", "")
                rowId = rowId.replace("'", "")
                columnFamily = columnFamily.replace("'", "")
                columnQualifier = columnQualifier.replace("'", "")
                
                table = next((table for table in TABLES if table.name == tableName), None)

                if not table:
                    return f"{tableName} table not found"
                    
                
                hf = HFile(table=table.name)
                
                if rowId in hf.rows and columnFamily in hf.rows[rowId] and columnQualifier in hf.rows[rowId][columnFamily]:
                    if timestamp in hf.rows[rowId][columnFamily][columnQualifier]:
                        hf.delete(rowId, columnFamily, columnQualifier, timestamp)
                    else:
                        del hf
                        return f"Timestamp not found. {timestamp} does not exist in row {rowId}, column {columnFamily}:{columnQualifier}.\n"
                
                del hf
                return f"Row {rowId} deleted from table {tableName}.\n"
            
            case "delete_all":
                # delete_all ’<table name>’, ’row id’
                tableName = spl[1]
                rowId = spl[2]

                if not table:
                    return f"{tableName} table not found"
                    
                if not valid_string(tableName):
                    return "Invalid table name"
                    

                if not valid_string(rowId):
                    return "Invalid row id"
                    

                tableName = tableName.replace("'", "")
                rowId = rowId.replace("'", "")
                table = next((table for table in TABLES if table.name == tableName), None)


                hf = HFile(table=table.name)

                if rowId in hf.rows:
                    hf.delete_all(rowId)
                
                del hf
                return f"All rows with id {rowId} deleted from table {tableName}.\n"
            
            case "count":
                # count '<table name>'
                tableName = spl[1]

                if not valid_string(tableName):
                    return "Invalid table name"
                    

                tableName = tableName.replace("'", "")
                table = next((table for table in TABLES if table.name == tableName), None)

                if table:
                    dataPath = os.path.join("tables", tableName, "regions", "region1", "hfile.json")
                    if os.path.exists(dataPath):
                        with open(dataPath, "r") as f:
                            data = json.load(f)
                            if data:
                                rowsN = len(data)
                                return f"{rowsN} row(s) found in table {tableName}."
                            else:
                                return "Table is empty."
                    else:
                        return f"No data found for {tableName} table."
                else:
                    return f"{tableName} table not found."
            
            case "truncate":
                # truncate '<table name>'
                tableName = spl[1]
                string = ""

                if not valid_string(tableName):
                    return "Invalid table name"
                    

                tableName = tableName.replace("'", "")
                table = next((table for table in TABLES if table.name == tableName), None)

                fams = list(table.column_families.keys())
                string += f"Truncating '{tableName}' table (it may take a while):\n"
                string += "- Disabling table...\n"
                table.disable() # Disable table
                t.sleep(20)
                table.drop() #drop table
                string += "- Truncating table...\n"
                create_table(tableName, fams, False) #create table
                
                return string

            case "insert_many":
                """
                insert_many ‘table name’  {’row ’,'colfamily:colname',’new value’} {’row ’,'colfamily:colname',’new value’} {’row ’,'colfamily:colname',’new value’}...{’row ’,'colfamily:colname',’new value’}
                """

                tableName = spl[1]
                
                inserts = spl[2:]  # ["{row', 'colfamily:colname', 'new', 'value}", "{row', 'colfamily:colname', 'new', 'value}"]
                
                for ins in inserts:
                    # Verificar que el insert sea válido
                    if not ins.startswith("{") or not ins.endswith("}"):
                        return "Invalid insert, missing '{' or '}'. Insert Many does not support spaces inside values."
                        

                if not valid_string(tableName):
                    return "Invalid table name"
                    

                tableName = tableName.replace("'", "")
                table = next((table for table in TABLES if table.name == tableName), None)

                if not table:
                    return f"{tableName} table not found"
                    

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
                            return "Invalid value"
                            

                    column_family = column.split(":")[0]
                    column_q = column.split(":")[1]

                    hf.put(row_id, column_family, column_q, value)

                del hf
                return f"Rows inserted in table {tableName}.\n"

            case "update_many":
                # update_many 'table name' 'cf:cq' value '1'  '2'  '3'
                tableName = spl[1]
                column = spl[2]
                value = spl[3]
                rows = spl[4:]

                if not valid_string(tableName):
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
                        return "Invalid row"
                        

                tableName = tableName.replace("'", "")
                column = column.replace("'", "")
                column_family = column.split(":")[0]
                column_q = column.split(":")[1]
                rows = [r.replace("'", "") for r in rows]
                

                table = next((table for table in TABLES if table.name == tableName), None)

                if not table:
                    return f"{tableName} table not found"
                    

                hf = HFile(table=table.name)

                for row in rows:
                    hf.put(row, column_family, column_q, value)

                del hf
                return f"Rows updated in table {tableName}.\n"
            
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

    result_text = tk.Text(main_frame, width=100, height=20, state=tk.DISABLED)
    result_text.grid(row=1, column=0, columnspan=3, sticky=(tk.W, tk.E, tk.N, tk.S))

    root.mainloop()

if __name__ == "__main__":
    run_gui()