from pydantic import BaseModel
import os
import json
import time

class Table(BaseModel):
    """"
    enabled: true,
    "name": "table1",
    "column_families": {
        "cf1": {
            "version": "1",
            ...
        },
    """
    enabled: bool = True
    name : str
    column_families: dict[str, dict[str, str]]

    def write_to_memory(self):
        # if not exists create folder table/{self.name}
        
        if not os.path.exists(f"tables/{self.name}"):
            os.makedirs(f"tables/{self.name}")

        # write the table to a file config.json
        with open(f"tables/{self.name}/config.json", "w") as f:
            json.dump(self.dict(), f)

        # create region
        if not os.path.exists(f"tables/{self.name}/regions"):
            os.makedirs(f"tables/{self.name}/regions")

        if not os.path.exists(f"tables/{self.name}/regions/region1"):
            os.makedirs(f"tables/{self.name}/regions/region1")

    #funciones de la clase
    def disable(self):
        """
        Disable the table
        """
        self.enabled = False
        self.write_to_memory()

    def enable(self):
        """
        Enable the table
        """
        self.enabled = True
        self.write_to_memory()

    def describe(self):
        """
        Return a string with the table description
        """
        return f"Table {self.name} is {'enabled' if self.enabled else 'disabled'}\n{self.name} \n{self.column_families}"
    
    def alter(self, column_family: str, version: str):
        """
        Alter the version of a column family
        """
        self.column_families[column_family]['version'] = version

    def drop(self):
        """
        Drop the table
        """
        os.remove(f"tables/{self.name}/config.json")
        os.rmdir(f"tables/{self.name}")

class HFile(BaseModel):
    table: str
    region: str = "region1"
    rows: dict[int, dict[str, dict[str, dict[str, str]]]] = {}

    """
    rows = 
    {
        "1": {
            "column_fam_1": {
                "column_qualifier": {
                    "timestamp": "1",
                }
            },
            "column_fam_2": {
                "column_qualifier": {
                    "timestamp": "1",
                }
            }
        },
    }
    """

    def __init__(self, table: str, region: str = "region1"):
        super().__init__(table=table, region=region)

        self.table = table
        self.region = region
        
        path = f"tables/{self.table}/regions/{self.region}"

        # check if hfile.json exists
        if os.path.exists(f"{path}/hfile.json"):
            print("hfile.json exists")
            with open(f"{path}/hfile.json", "r") as f:
                self.rows = json.load(f)
        else:
            self.rows = {}
            # create hfie.json
            with open(f"{path}/hfile.json", "w") as f:
                json.dump(self.rows, f)

    def write_to_memory(self):
        path = f"tables/{self.table}/regions/{self.region}"
        with open(f"{path}/hfile.json", "w") as f:
            json.dump(self.rows, f)

    def put(self, row: int, column_family: str, column_qualifier: str, value: str):
        ts = str(int(time.time()))
        if row not in self.rows:
            self.rows[row] = {}
        if column_family not in self.rows[row]:
            self.rows[row][column_family] = {}
        if column_qualifier not in self.rows[row][column_family]:
            self.rows[row][column_family][column_qualifier] = {}
        self.rows[row][column_family][column_qualifier][ts] = value
        self.write_to_memory()

    def get(self, row: int, column_family: str, column_qualifier: str, timestamp: str):
        return self.rows[row][column_family][column_qualifier][timestamp]
    
    def delete(self, row: int, column_family: str, column_qualifier: str):
        del self.rows[row][column_family][column_qualifier]
        self.write_to_memory()

    def scan(self):
        return self.rows

