from models.table import Table
import json
import os

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
    print(f"Creando tabla {table_name} con columnas {columns}")

    table = Table(name=table_name, column_families={column: {"version": "1"} for column in columns})

    for k,v in table.column_families.items():
        print(k,v)

    table.write_to_memory()

def main():
    global TABLES

    init()

    for table in TABLES:
        print(table.name)

    input_command = ""

    while input_command != "exit":
        input_command = input("->")

        if input_command == "exit":
            exit()

        spl = input_command.split()

        match spl[0]:
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

            case "describe":
                table_name = spl[1]
                table = next((table for table in TABLES if table.name == table_name), None)
                print(table.describe())

            case _:
                print("Comando no reconocido")


if __name__ == "__main__":
    main()