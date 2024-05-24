import tkinter as tk
from tkinter import Text

# Importar funciones desde main.py
from models.table import Table, HFile
import shutil
import json
import os
import re
import time as t

from main import (
    init,
    list_tables,
    create_table,
    valid_string,
    # Agrega aquí todas las demás funciones que necesites
)

TABLES = []

def executeCode(event=None):
    code = terminal.get("insert linestart", "insert lineend").strip()
    if code:
        terminal.insert(tk.END, f"\n{code}\n")
        resultado = executeCommand(code)
        terminal.insert(tk.END, resultado)
    terminal.insert(tk.END, "\n[cloudera@quickstart] ")
    terminal.mark_set("insert", "end-1c lineend")
    return 'break'

def executeCommand(comando):
    global TABLES
    spl = comando.split()
    cmd = spl[0]
    args = spl[1:]

    try:
        if cmd == "create":
            table_n = args[0]
            columns = args[1:]
            if not valid_string(table_n):
                return "Invalid table name\n"
            table_n = table_n.replace("'", "")
            if any([not valid_string(column) for column in columns]):
                return "Invalid column family name\n"
            columns = [column.replace("'", "") for column in columns]
            create_table(table_n, columns, True)
            return "Table created successfully\n"
        elif cmd == "list":
            list_tables()
            return "Tables listed successfully\n"
        # Agregar más casos según las funciones del main
        else:
            return "Unrecognized command\n"
    except Exception as e:
        return str(e) + "\n"

root = tk.Tk()
root.title("HBase Simulator")

# Configuración de la terminal
terminal_frame = tk.Frame(root, height=100)
terminal_frame.pack(fill="both", expand=True)

terminal = Text(terminal_frame, bg="black", fg="white", insertbackground="white")
terminal.pack(fill="both", expand=True)

# Insertar el prompt
terminal.insert(tk.END, "[cloudera@quickstart] ")

# Ejecutar el comando cuando se presiona la tecla Enter
terminal.bind("<Return>", executeCode)

root.mainloop()