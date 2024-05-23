from models.table import Table, HFile
import shutil
import json
import os
import re
import pandas as pd

TABLES = []

def load_table(table_name: str) -> Table:
    # from a json like this {"enabled": true, "name": "tabla", "column_families": {"c1": {"version": "1"}, "c2": {"version": "1"}}}

    json_path = f"tables/{table_name}/config.json"

    with open(json_path, "r") as f:
        data = json.load(f)
        table = Table(name=data['name'], column_families=data['column_families'])
        table.enabled = data['enabled']
        
    return table


def init() -> None:
    global TABLES

    if not os.path.exists("tables"):
        os.makedirs("tables")

    table_names = os.listdir("tables")
    tables = []

    for table in table_names:
        tables.append(load_table(table))

    TABLES = tables

    
def list_tables():
    tables = os.listdir("tables")

    for table in tables:
        print(table)


def create_table(table_name: str, columns: list[str]) -> None:
    global TABLES
    print(f"Creando tabla {table_name} con columnas {columns}")

    table = Table(name=table_name, column_families={column: {"version": "1"} for column in columns})

    for k,v in table.column_families.items():
        print(k,v)

    table.write_to_memory()

    TABLES.append(table)
    

def main():
    global TABLES

    init()

    for table in TABLES:
        print(table.name)

    input_command = ""

    while input_command != "exit":
        input_command = input("-> ")

        if input_command == "exit":
            exit()

        spl = input_command.split()

        match spl[0]:
            # --- DDL Functions ---
            case "create":
                print("create")
                table_n = spl[1]
                columns = spl[2:]
                create_table(table_n, columns)
                
            case "list":
                list_tables()
                
            case "disable":
                table_name = spl[1]
                table = next((table for table in TABLES if table.name == table_name), None)
                table.disable()
                
            case "enable":
                table_name = spl[1]
                table = next((table for table in TABLES if table.name == table_name), None)
                table.enable()
                
            case "is_disabled":
                table_name = spl[1]
                table = next((table for table in TABLES if table.name == table_name), None)
                print(not table.enabled)
                
            case "alter":
                table_name = spl[1]
                column_family = spl[2]
                version = spl[3]
                table = next((table for table in TABLES if table.name == table_name), None)
                
                table.alter(column_family, version)
                table.write_to_memory()
                
            case "drop":
                table_name = spl[1]
                table = next((table for table in TABLES if table.name == table_name), None)
                table.drop()
                
            case "drop_all":
                regex = spl[1]
                pattern = re.compile(regex)
                table_names = os.listdir("tables")
                
                for table_name in table_names:
                    if pattern.match(table_name):
                        table_path = os.path.join("tables", table_name)
                        if os.path.isdir(table_path): 
                            shutil.rmtree(table_path)
                            print(f"{table_name} table(s) dropped")
                        else:
                            os.remove(table_path)
                            print(f"{table_name} table(s) dropped")

            case "describe":
                table_name = spl[1]
                table = next((table for table in TABLES if table.name == table_name), None)
                print(table.describe())
                
            # --- DML Functions ---
            case "put":
                # put table_name row_id column_family:column_q value
                table_name = spl[1]
                row_id = spl[2]
                column = spl[3]
                value = spl[4]

                column_family = column.split(":")[0]
                column_q = column.split(":")[1]

                table = next((table for table in TABLES if table.name == table_name), None)

                if column_family not in table.column_families:  
                    print("Column family not found")
                    break

                hf = HFile(table=table.name)
                hf.put(row_id, column_family, column_q, value)

            case "get":
                # get ’<table name>’, ’row id’
                """
                COLUMN                        CELL
                details:name                  timestamp=123456789, value=John
                details:age                   timestamp=123456789, value=30
                """
                table_name = spl[1]
                row_id = spl[2]

                print("spl", spl)
                
                if len(spl) > 3:
                    """
                    COLUMN                              CELL
                    personal_data:pet                   timestamp=123456789, value=dog
                    """
                    #spl ['get', 'tabla4', '1', '{COLUMN', '=>', 'fam:cq}']
                    # get ’<table name>’, ’row id’, ’{COLUMN => column family:column qualifier}’

                    if spl[3] == "{COLUMN":
                        column_f = spl[5].split(":")[0]
                        column_q = spl[5].split(":")[1]
                        column_q = column_q[:-1]  # Eliminar el último caracter que es el '}'

                    else:
                        print("Invalid command")

                else:
                    # get ’<table name>’, ’row id’
                    column_f = None
                    column_q = None

                table = next((table for table in TABLES if table.name == table_name), None)
                
                if table is not None:
                    hf = HFile(table=table.name)
                    row_data = hf.get(row_id)
                    
                    print(f"COLUMN\t\t\t\tCELL")
                    
                    if column_f is None and column_q is None:
                        for column_family, columns in row_data.items():
                            for column, value in columns.items():
                                keys = list(value.keys())
                                first = max(keys)
                                print(f"{column_family}:{column}\t\t\t\ttimestamp={first},value={value[first]}")

                    else:
                        for column_family, columns in row_data.items():
                            for column, value in columns.items():
                                if column_f == column_family and column_q == column:
                                    keys = list(value.keys())
                                    first = max(keys)
                                    print(f"{column_family}:{column}\t\t\t\ttimestamp={first},value={value[first]}")
                else:
                    print("Table not found")
                
            case "scan":
                table_name = spl[1]
                table = next((table for table in TABLES if table.name == table_name), None)
                
                hf = HFile(table=table.name)
                print(hf.scan())
                
            case "delete":
                tableName = spl[1]
                rowId = spl[2]
                column = spl[3]
                timestamp = spl[4]
                
                columnFamily = column.split(":")[0]
                columnQualifier = column.split(":")[1]
                
                table = next((table for table in TABLES if table.name == tableName), None)
                
                if not table:
                    print(f"{tableName} table not found")
                
                hf = HFile(table=table.name)
                
                if rowId in hf.rows and columnFamily in hf.rows[rowId] and columnQualifier in hf.rows[rowId][columnFamily]:
                    if timestamp in hf.rows[rowId][columnFamily][columnQualifier]:
                        del hf.rows[rowId][columnFamily][columnQualifier][timestamp]
                        hf.write_to_memory()
                        print(f"Deleted {columnFamily}:{columnQualifier} from row {rowId}")
                    else:
                        print(f"Timestamp {timestamp} not found in {rowId}, {columnFamily}:{columnQualifier}")
                else:
                    print(f"Row {rowId} or {columnFamily}:{columnQualifier} not found")
                
                if not hf.rows[rowId][columnFamily][columnQualifier]:
                    del hf.rows[rowId][columnFamily][columnQualifier]
                
                if not hf.rows[rowId][columnFamily]:
                    del hf.rows[rowId][columnFamily]
                
                if not hf.rows[rowId]:
                    del hf.rows[rowId]
                    
                hf.write_to_memory()
            
            case "delete_all":
                pass
            
            case "count":
                tableName = spl[1]
                table = next((table for table in TABLES if table.name == tableName), None)
                if table:
                    dataPath = os.path.join("tables", tableName, "regions", "region1", "hfile.json")
                    if os.path.exists(dataPath):
                        with open(dataPath, "r") as f:
                            data = json.load(f)
                            if data:
                                rowsN = len(data)
                                print(f"{rowsN} row(s) found in table {tableName}.")
                            else:
                                print("Table is empty.")
                    else:
                        print(f"No data found for {tableName} table.")
                else:
                    print(f"{tableName} table not found.")
            
            case "truncate":
                pass
            
            case _:
                print("Unrecognized command")


if __name__ == "__main__":
    main()