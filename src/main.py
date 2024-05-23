from models.table import Table, HFile
import shutil
import json
import os
import re

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
                table_name = spl[1]
                row_id = spl[2]
                

                pass
                
            case "scan":
                table_name = spl[1]
                table = next((table for table in TABLES if table.name == table_name), None)
                
                hf = HFile(table=table.name)
                print(hf.scan())
            case "delete":
                pass
            
            case "delete_all":
                pass
            
            case "count":
                pass
            
            case "truncate":
                pass
            
            case _:
                print("Comando no reconocido")


if __name__ == "__main__":
    main()