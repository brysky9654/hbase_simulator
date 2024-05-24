from models.table import Table, HFile
import shutil
import json
import os
import re
import time as t

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


def create_table(table_name: str, columns: list[str], printdata:bool) -> None:
    global TABLES
    table = Table(name=table_name, column_families={column: {"version": "1"} for column in columns})

    if printdata:
        print(f"=> Hbase:: Table - {table_name}")

    table.write_to_memory()

    TABLES.append(table)


def valid_string(s: str) -> bool:
    # si la cadena está limitada por comillas simples
    if s[0] == "'" and s[-1] == "'":
        return True
    
    return False
    

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

        spl = input_command.split(" ")

        match spl[0]:
            # --- DDL Functions ---
            case "create":
                # create ’<table name>’, ’<column family 1>’, ’<column family 2>’, ...
                print("create")
                table_n = spl[1]
                columns = spl[2:]

                if not valid_string(table_n):
                    print("Invalid table name")
                    continue

                table_n = table_n.replace("'", "")

                if any([not valid_string(column) for column in columns]):
                    print("Invalid column family name")
                    continue
                    
                columns = [column.replace("'", "") for column in columns]

                create_table(table_n, columns, True)
                
            case "list":
                # list
                list_tables()
                
            case "disable":
                # disable ’<table name>’
                table_name = spl[1]
                
                if not valid_string(table_name):
                    print("Invalid table name")
                    continue
                
                if not table:
                    print(f"{table_name} table not found")
                    continue

                table_name = table_name.replace("'", "")
                table = next((table for table in TABLES if table.name == table_name), None)

                table.disable()
                
            case "enable":
                # enable ’<table name>’
                table_name = spl[1]

                if not valid_string(table_name):
                    print("Invalid table name")
                    continue

                table_name = table_name.replace("'", "")

                table = next((table for table in TABLES if table.name == table_name), None)
                if not table:
                    print(f"{table_name} table not found")
                    continue

                table.enable()
                
            case "is_disabled":
                # is_disabled ’<table name>’
                table_name = spl[1]

                if not valid_string(table_name):
                    print("Invalid table name")
                    continue

                table_name = table_name.replace("'", "")

                table = next((table for table in TABLES if table.name == table_name), None)

                if not table:
                    print(f"{table_name} table not found")
                    continue
                
                print(not table.enabled)
                
            case "alter":
                # alter ’<table name>’, ’<column family>’, ’<version>’
                table_name = spl[1]
                column_family = spl[2]
                version = spl[3]
                
                if not valid_string(table_name):
                    print("Invalid table name")
                    continue

                if not valid_string(column_family):
                    print("Invalid column family name")
                    continue

                if not valid_string(version):
                    print("Invalid version")
                    continue
                
                if not table:
                    print(f"{table_name} table not found")
                    continue

                table_name = table_name.replace("'", "")
                column_family = column_family.replace("'", "")
                version = version.replace("'", "")

                table = next((table for table in TABLES if table.name == table_name), None)
                
                table.alter(column_family, version)
                table.write_to_memory()
                
            case "drop":
                # drop ’<table name>’
                table_name = spl[1]
                
                if not valid_string(table_name):
                    print("Invalid table name")
                    continue

                if not table:
                    print(f"{table_name} table not found")
                    continue

                table_name = table_name.replace("'", "")

                table = next((table for table in TABLES if table.name == table_name), None)
                
                table.drop()
                
            case "drop_all":
                # drop_all ’<regex>’
                regex = spl[1]
                if not valid_string(regex):
                    print("Invalid regex")
                    continue

                regex = regex.replace("'", "")

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
                # describe ’<table name>’
                table_name = spl[1]

                if not valid_string(table_name):    
                    print("Invalid table name")
                    continue

                if not table:
                    print(f"{table_name} table not found")
                    continue

                table_name = table_name.replace("'", "")

                table = next((table for table in TABLES if table.name == table_name), None)

                print(table.describe())
                
            # --- DML Functions ---
            case "put":
                # put ‘table name’, ’row ’, 'colfamily:colname', ’new value’
                table_name = spl[1]
                row_id = spl[2]
                column = spl[3]
                value = spl[4]

                if not valid_string(table_name):
                    print("Invalid table name")
                    continue

                if not valid_string(row_id):
                    print("Invalid row id")
                    continue

                if not valid_string(column):
                    print("Invalid column")
                    continue

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
                    print("Column family not found")
                    break
                
                if not table:
                    print(f"{table_name} table not found")
                    continue

                hf = HFile(table=table.name)
                hf.put(row_id, column_family, column_q, value)

                del hf

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
                    print("Invalid table name")
                    continue

                if not valid_string(row_id):
                    print("Invalid row id")
                    continue

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
                            print("Invalid column")
                            continue
                        
                        column_f = spl[5].split(":")[0].replace("'", "")
                        column_q = spl[5].split(":")[1].replace("'", "")
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
                    
                    print(f"{'COLUMN'.ljust(40)} CELL")
                    
                    if column_f is None and column_q is None:
                        for column_family, columns in row_data.items():
                            for column, value in columns.items():
                                keys = list(value.keys())
                                first = max(keys)
                                s = f"{column_family}:{column}"
                                print(f"{s.ljust(40)} timestamp={first},value={value[first]}")

                    else:
                        for column_family, columns in row_data.items():
                            for column, value in columns.items():
                                if column_f == column_family and column_q == column:
                                    keys = list(value.keys())
                                    first = max(keys)
                                    s = f"{column_family}:{column}"
                                    print(f"{s.ljust(40)} timestamp={first},value={value[first]}")
                else:
                    print("Table not found")
                    continue

                del hf
                
            case "scan":
                # scan ’<table name>’
                table_name = spl[1]

                if not table:
                    print(f"{table_name} table not found")
                    continue

                if not valid_string(table_name):
                    print("Invalid table name")
                    continue

                table_name = table_name.replace("'", "")

                table = next((table for table in TABLES if table.name == table_name), None)
                
                hf = HFile(table=table.name)
                rows = hf.scan()
                """
                ROW                              COLUMN+CELL
                1                                column=column_fam:column_qualifier, timestamp=123456789, value=value
                """
                print(f"{'ROW'.ljust(40)} COLUMN+CELL")
                for row, columns in rows.items():
                    for column_family, columns in columns.items():
                        for column, value in columns.items():
                            keys = list(value.keys())
                            first = max(keys)
                            s = f"{row}"
                            print(f"{s.ljust(40)} column={column_family}:{column}, timestamp={first}, value={value[first]}")
                del hf
                
            case "delete":
                # delete ’<table name>’, ’row id’, ’column family:column qualifier’, timestamp
                tableName = spl[1]
                rowId = spl[2]
                column = spl[3]
                timestamp = spl[4]
                
                
                if not table:
                    print(f"{tableName} table not found")
                    continue

                if not valid_string(tableName):
                    print("Invalid table name")
                    continue

                if not valid_string(rowId):
                    print("Invalid row id")
                    continue

                if not valid_string(column):
                    print("Invalid column")
                    continue


                columnFamily = column.split(":")[0]
                columnQualifier = column.split(":")[1]


                tableName = tableName.replace("'", "")
                rowId = rowId.replace("'", "")
                columnFamily = columnFamily.replace("'", "")
                columnQualifier = columnQualifier.replace("'", "")
                
                table = next((table for table in TABLES if table.name == tableName), None)

                if not table:
                    print(f"{tableName} table not found")
                    continue
                
                hf = HFile(table=table.name)
                
                if rowId in hf.rows and columnFamily in hf.rows[rowId] and columnQualifier in hf.rows[rowId][columnFamily]:
                    if timestamp in hf.rows[rowId][columnFamily][columnQualifier]:
                        hf.delete(rowId, columnFamily, columnQualifier, timestamp)
                del hf
            
            case "delete_all":
                # delete_all ’<table name>’, ’row id’
                tableName = spl[1]
                rowId = spl[2]

                if not table:
                    print(f"{tableName} table not found")
                    continue

                if not valid_string(tableName):
                    print("Invalid table name")
                    continue

                if not valid_string(rowId):
                    print("Invalid row id")
                    continue

                tableName = tableName.replace("'", "")
                rowId = rowId.replace("'", "")
                table = next((table for table in TABLES if table.name == tableName), None)


                hf = HFile(table=table.name)

                if rowId in hf.rows:
                    hf.delete_all(rowId)
                
                del hf
            
            case "count":
                # count ’<table name>’
                tableName = spl[1]

                if not valid_string(tableName):
                    print("Invalid table name")
                    continue

                tableName = tableName.replace("'", "")
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
                # truncate ’<table name>’
                tableName = spl[1]

                if not valid_string(tableName):
                    print("Invalid table name")
                    continue

                tableName = tableName.replace("'", "")
                table = next((table for table in TABLES if table.name == tableName), None)

                fams = list(table.column_families.keys())
                print(f"Truncating '{tableName}' table (it may take a while):")
                print("- Disabling table...")
                table.disable() # Disable table
                t.sleep(40)
                table.drop() #drop table
                print("- Truncating table...")
                create_table(tableName, fams, False) #create table

            case "insert_many":
                """
                insert_many ‘table name’  {’row ’,'colfamily:colname',’new value’} {’row ’,'colfamily:colname',’new value’} {’row ’,'colfamily:colname',’new value’}...{’row ’,'colfamily:colname',’new value’}
                """

                tableName = spl[1]
                
                inserts = spl[2:]  # ["{row', 'colfamily:colname', 'new', 'value}", "{row', 'colfamily:colname', 'new', 'value}"]
                
                for ins in inserts:
                    # Verificar que el insert sea válido
                    if not ins.startswith("{") or not ins.endswith("}"):
                        print("Invalid insert, missing '{' or '}'. Insert Many does not support spaces inside values.")
                        continue

                if not valid_string(tableName):
                    print("Invalid table name")
                    continue

                tableName = tableName.replace("'", "")
                table = next((table for table in TABLES if table.name == tableName), None)

                if not table:
                    print(f"{tableName} table not found")
                    continue

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
                            print("Invalid value")
                            continue



                    column_family = column.split(":")[0]
                    column_q = column.split(":")[1]

                    hf.put(row_id, column_family, column_q, value)

                del hf

            case "update_many":
                # update_many 'table name' 'cf:cq' value '1'  '2'  '3'
                tableName = spl[1]
                column = spl[2]
                value = spl[3]
                rows = spl[4:]

                if not valid_string(tableName):
                    print("Invalid table name")
                    continue

                if not valid_string(column):
                    print("Invalid column")
                    continue
                
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
                        print("Invalid row")
                        continue

                tableName = tableName.replace("'", "")
                column = column.replace("'", "")
                column_family = column.split(":")[0]
                column_q = column.split(":")[1]
                rows = [r.replace("'", "") for r in rows]
                

                table = next((table for table in TABLES if table.name == tableName), None)

                if not table:
                    print(f"{tableName} table not found")
                    continue

                hf = HFile(table=table.name)

                for row in rows:
                    hf.put(row, column_family, column_q, value)

                del hf
            
            case _:
                print("Unrecognized command")


if __name__ == "__main__":
    main()