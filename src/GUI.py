import tkinter as tk
from tkinter import Text
import subprocess

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
    try:
        resultado = subprocess.run(comando, shell=True, capture_output=True, text=True)
        return resultado.stdout + resultado.stderr
    except Exception as e:
        return str(e)

root = tk.Tk()
root.title("HBase Simulator")

# config
terminal_frame = tk.Frame(root, height=100)
terminal_frame.pack(fill="both", expand=True)

terminal = Text(terminal_frame, bg="black", fg="white", insertbackground="white")
terminal.pack(fill="both", expand=True)

# insert the prompt
terminal.insert(tk.END, "[cloudera@quickstart] ")

# execute the command when the Enter key is pressed
terminal.bind("<Return>", executeCode)

root.mainloop()