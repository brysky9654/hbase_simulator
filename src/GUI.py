import tkinter as tk
from tkinter import Text

def ejecutar_codigo(event=None):
    code = terminal.get("insert linestart", "insert lineend")
    code = code.replace("[cloudera@quickstart]", "")
    terminal.insert(tk.END, f"\n{code}") #reemplazar por las funciones

    terminal.insert(tk.END, "\n[cloudera@quickstart] ")
    # Mover el cursor justo después del prompt
    terminal.mark_set("insert", "end-1c lineend")
    return 'break'  # Prevenir que la tecla Enter genere una nueva línea automáticamente

root = tk.Tk()
root.title("Terminal Cloudera")

# Configuración del frame de la terminal
terminal_frame = tk.Frame(root, height=100)
terminal_frame.pack(fill="both", expand=True)

terminal = Text(terminal_frame, bg="black", fg="white")
terminal.pack(fill="both", expand=True)

# Insertar el prompt inicial
terminal.insert(tk.END, "[cloudera@quickstart] ")

# Enlazar la tecla Enter a la función ejecutar_codigo
terminal.bind("<Return>", ejecutar_codigo)

root.mainloop()
