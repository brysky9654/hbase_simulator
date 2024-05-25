import pandas as pd

from models.table import Table, HFile

# Load the CSV file
employee_data = pd.read_csv("./data/employee_data.csv")

# Create an HFile instance for employee data
table = Table(name="employee_table", column_families={"PersonalInfo": {"version": "3"}, "JobInfo": {"version": "3"}})
table.write_to_memory()

employee_hfile = HFile(table="employee_table")

# Insert data from the DataFrame into the HFile
for index, row in employee_data.iterrows():
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
employee_hfile.write_to_memory()