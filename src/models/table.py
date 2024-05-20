from pydantic import BaseModel
import os
import json

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

        
    