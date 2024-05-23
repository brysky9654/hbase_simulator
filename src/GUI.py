import tkinter as tk
from tkinter import filedialog, messagebox, Text
from collections import Counter


# Función para resetear el editor
def reset_editor():
    global archivo_actual
    texto.delete(1.0, tk.END)
    actualizar_lineas()

# Función para ejecutar el código
def ejecutar_codigo():
    code = texto.get(1.0, tk.END)  
    terminal.delete(1.0, tk.END)  

    try:
        terminal.insert(tk.END, code)  
    except Exception as e:
        messagebox.showerror("Error", e)                    

# Función para actualizar los números de línea
def actualizar_lineas(event=None):
    lineas.config(state="normal",fg="black")
    lineas.delete("1.0", "end")
    num = texto.count("1.0", "end", "displaylines")[0]
    lineas.insert("1.0", "\n".join(str(i) for i in range(1, num + 1)))
    lineas.config(state="disabled")

def cambiar_modo(nuevo_modo):
    global modo_actual
    modo_actual = nuevo_modo
    root.title(f"{modo_actual} - {archivo_actual if archivo_actual else ''}")


root = tk.Tk()
root.title("Editor")

# Encabezado para cambiar entre modos
modo_frame = tk.Frame(root)
modo_frame.grid(row=0, column=0, columnspan=2, sticky="ew")

# Botones para las funcionalidades deben estar en fila
botones_frame = tk.Frame(root)
botones_frame.grid(row=1, column=0, columnspan=2, sticky="ew")

# Botón para resetear el editor
boton_reset = tk.Button(botones_frame, text="Reset", command=reset_editor)
boton_reset.pack(side="left")

# Botón para ejecutar el código
boton_ejecutar = tk.Button(botones_frame, text="▶", command=ejecutar_codigo)
boton_ejecutar.pack(side="left")

editor_frame = tk.Frame(root)
editor_frame.grid(row=2, column=0, sticky="nsew")

# Configuración del frame del editor y los números de línea
lineas_frame = tk.Frame(editor_frame)
lineas_frame.pack(side="left", fill="y")

lineas = tk.Text(lineas_frame, width=4, padx=3, takefocus=0, border=0, background="lightgrey", state="disabled", wrap="none")
lineas.pack(side="top", fill="y")

scrollbar = tk.Scrollbar(editor_frame)
scrollbar.pack(side="right", fill="y")

texto = Text(editor_frame, yscrollcommand=scrollbar.set, wrap="none")
texto.pack(side="left", fill="both", expand=True)
scrollbar.config(command=texto.yview)

texto.bind("<KeyRelease>", actualizar_lineas)
texto.bind("<MouseWheel>", actualizar_lineas)


# Configuración del frame de la terminal
terminal_frame = tk.Frame(root, height=100)
terminal_frame.grid(row=3, column=0, columnspan=2, sticky="ew")

terminal = Text(terminal_frame, bg="black", fg="white")
terminal.pack(fill="both", expand=True)

root.grid_rowconfigure(2, weight=1)
root.grid_columnconfigure(0, weight=2)
root.grid_columnconfigure(1, weight=1)


if __name__ == '__main__':
    print("Iniciando GUI")
    root.mainloop()
