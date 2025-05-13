# GUI moderna para el procesador contable con ttkbootstrap y consola integrada
# Ejecuta este script con: python interfaz_gui.py
#pyinstaller --noconsole --onefile --name PCA interfaz_gui.py

import os
import sys
import threading
import subprocess
import ttkbootstrap as ttk
from ttkbootstrap.constants import *
from tkinter import filedialog, messagebox, scrolledtext

class ConsoleRedirect:
    def __init__(self, text_widget):
        self.text_widget = text_widget

    def write(self, message):
        self.text_widget.configure(state='normal')
        self.text_widget.insert('end', message)
        self.text_widget.see('end')
        self.text_widget.configure(state='disabled')

    def flush(self):
        pass

class App(ttk.Window):
    def __init__(self):
        super().__init__(themename="solar")
        self.title("Procesador Contable - Acertijo SA")
        self.geometry("800x600")
        self.resizable(False, False)

        self.path_var = ttk.StringVar()
        self.create_widgets()
        self.setup_console_redirect()

    def create_widgets(self):
        ttk.Label(self, text="Ruta base de trabajo:", font=("Segoe UI", 12)).pack(pady=10)

        frame = ttk.Frame(self)
        frame.pack(pady=5)

        entry = ttk.Entry(frame, textvariable=self.path_var, width=60)
        entry.pack(side=LEFT, padx=5)

        browse_btn = ttk.Button(frame, text="Buscar...", command=self.seleccionar_carpeta)
        browse_btn.pack(side=LEFT)

        self.progress = ttk.Progressbar(self, length=500, mode='indeterminate')
        self.progress.pack(pady=15)

        self.run_btn = ttk.Button(self, text="Ejecutar procesamiento", bootstyle=PRIMARY, command=self.ejecutar)
        self.run_btn.pack(pady=5)

        ttk.Label(self, text="Salida del proceso:", font=("Segoe UI", 10)).pack(pady=5)
        self.console = scrolledtext.ScrolledText(self, height=15, width=85, state='disabled', font=('Courier New', 9))
        self.console.pack(pady=5)

    def setup_console_redirect(self):
        sys.stdout = ConsoleRedirect(self.console)
        sys.stderr = ConsoleRedirect(self.console)

    def seleccionar_carpeta(self):
        carpeta = filedialog.askdirectory()
        if carpeta:
            self.path_var.set(carpeta)

    def ejecutar(self):
        ruta = self.path_var.get().strip()
        if not ruta or not os.path.isdir(ruta):
            messagebox.showerror("Error", "Selecciona una carpeta válida.")
            return

        self.progress.start()
        self.run_btn.config(state=DISABLED)

        threading.Thread(target=self.procesar, args=(ruta,), daemon=True).start()

    def procesar(self, ruta):
        try:
            gui_flag_path = os.path.join(ruta, "__modo_gui__")
            with open(gui_flag_path, 'w') as f:
                f.write("true")

            comando = [sys.executable, "procesador_contable.py", ruta]
            proceso = subprocess.Popen(
                comando,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                bufsize=1,
                universal_newlines=True
            )

            for linea in proceso.stdout:
                print(linea, end="")

            proceso.wait()

            if os.path.exists(gui_flag_path):
                os.remove(gui_flag_path)

            messagebox.showinfo("Listo", "Procesamiento completado con éxito.")

        except Exception as e:
            messagebox.showerror("Error", f"Hubo un problema: {str(e)}")
        finally:
            self.progress.stop()
            self.run_btn.config(state=NORMAL)

if __name__ == "__main__":
    app = App()
    app.mainloop()
