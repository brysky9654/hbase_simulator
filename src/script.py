import pandas as pd
from models.table import Table, HFile

def load_data_1():
    # Load the CSV file
    employee_data = pd.read_csv("./data/employee_data.csv")

    # Create an HFile instance for employee data
    table = Table(name="employee_table", column_families={"PersonalInfo": {"version": "3"}, "JobInfo": {"version": "3"}})
    table.write_to_memory()

    employee_hfile = HFile(table="employee_table")

    # Insert data from the DataFrame into the HFile
    for index, row in employee_data.iterrows():
        # Progress in percentage, each 10%
        if index % (len(employee_data) // 10) == 0:
            print(f"Progress: {round((index + 1) / len(employee_data) * 100, 2)}%", end="\r")
        
        emp_id = str(row['EmpID'])  # Use EmpID as the row key
        personal_info = ['FirstName', 'LastName', 'DOB', 'GenderCode', 'RaceDesc', 'MaritalDesc']
        job_info = ['StartDate', 'Title', 'Supervisor', 'ADEmail', 'BusinessUnit', 'EmployeeStatus', 'EmployeeType', 'PayZone']

        for field in personal_info:
            if (type(row[field]) != str) and (type(row[field]) != int) and (type(row[field]) != float):
                row[field] = ""
                
            employee_hfile.put(emp_id, 'PersonalInfo', field, row[field])
            

        for field in job_info:
            if (type(row[field]) != str) and (type(row[field]) != int) and (type(row[field]) != float):
                row[field] = ""
            
            employee_hfile.put(emp_id, 'JobInfo', field, row[field])

    # Write data to file
    # employee_hfile.write_to_memory() 


"""
Column Family 1: Producto
Column Qualifiers: IDProducto, NombreProducto, Precio
Column Family 2: Stock
Column Qualifiers: CantidadActual, CantidadMínima
Column Family 3: Proveedor
Column Qualifiers: NombreProveedor, Teléfono
"""

def load_data_2():
    entrepenur_data = pd.read_csv("./data/entrepreneur_data.csv")

    table = Table(name="entrepreneur_table", column_families={"Producto": {"version": "3"}, "Stock": {"version": "3"}, "Proveedor": {"version": "3"}})
    table.write_to_memory()

    entrepreneur_hfile = HFile(table="entrepreneur_table")

    len_data = len(entrepenur_data)

    for index, row in entrepenur_data.iterrows():

        # Progress in percentage, each 10%
        if index % (len_data // 10) == 0:
            print(f"Progress: {round((index + 1) / len_data * 100, 2)}%", end="\r")

        product_id = str(row['IDProducto'])
            
        product_info = ['NombreProducto', 'Precio']
        stock_info = ['CantidadActual', 'CantidadMinima']
        provider_info = ['NombreProveedor', 'Telefono']

        for field in product_info:
            if (type(row[field]) != str) and (type(row[field]) != int) and (type(row[field]) != float):
                row[field] = ""
                
            entrepreneur_hfile.put(product_id, 'Producto', field, row[field])

        for field in stock_info:
            if (type(row[field]) != str) and (type(row[field]) != int) and (type(row[field]) != float):
                row[field] = ""
                
            entrepreneur_hfile.put(product_id, 'Stock', field, row[field])

        for field in provider_info:
            if (type(row[field]) != str) and (type(row[field]) != int) and (type(row[field]) != float):
                row[field] = ""
                
            entrepreneur_hfile.put(product_id, 'Proveedor', field, row[field])

"""
Column Family 1: Usuarios
Column Qualifiers: ID, Nombre, Apellido
Column Family 2: Fechas
Column Qualifiers: FechaAcceso, HoraAcceso
Column Family 3: Dispositivos
Column Qualifiers: TipoDispositivo, IDDispositivo
"""
def load_data_3():
    access_data = pd.read_csv("./data/access_control.csv")

    table = Table(name="access_table", column_families={"Usuarios": {"version": "3"}, "Fechas": {"version": "3"}, "Dispositivos": {"version": "3"}})
    table.write_to_memory()

    access_hfile = HFile(table="access_table")

    len_data = len(access_data)

    for index, row in access_data.iterrows():
            
            # Progress in percentage, each 10%
            if index % (len_data // 10) == 0:
                print(f"Progress: {round((index + 1) / len_data * 100, 2)}%", end="\r")
    
            user_id = str(row['ID'])
                
            user_info = ['Nombre', 'Apellido']
            date_info = ['FechaAcceso', 'HoraAcceso']
            device_info = ['TipoDispositivo', 'IDDispositivo']
    
            for field in user_info:
                if (type(row[field]) != str) and (type(row[field]) != int) and (type(row[field]) != float):
                    row[field] = ""
                    
                access_hfile.put(user_id, 'Usuarios', field, row[field])
    
            for field in date_info:
                if (type(row[field]) != str) and (type(row[field]) != int) and (type(row[field]) != float):
                    row[field] = ""
                    
                access_hfile.put(user_id, 'Fechas', field, row[field])
    
            for field in device_info:
                if (type(row[field]) != str) and (type(row[field]) != int) and (type(row[field]) != float):
                    row[field] = ""
                    
                access_hfile.put(user_id, 'Dispositivos', field, row[field])



if __name__ == "__main__":
    # load_data_1()
    
    print("\nLoading data 2\n")
    load_data_2()
    
    #print("\nLoading data 3\n")
    #load_data_3()