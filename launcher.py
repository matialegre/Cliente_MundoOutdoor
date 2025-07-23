#!/usr/bin/env python3
"""
🚀 MUNDO OUTDOOR LAUNCHER
Lanzador visual hermoso para el sistema de gestión MercadoLibre
"""
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import subprocess
import sys
from pathlib import Path
import os
from PIL import Image, ImageTk
import json

class MundoOutdoorLauncher:
    """Launcher visual hermoso para el sistema MUNDO OUTDOOR."""
    
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("🏔️ MUNDO OUTDOOR Launcher")
        self.root.geometry("800x600")
        self.root.resizable(True, True)
        
        # Configuración de paths
        self.base_path = Path(__file__).parent
        self.config_file = self.base_path / "launcher_config.json"
        
        # Cargar configuración
        self.config = self.load_config()
        
        # Configurar estilo
        self.setup_style()
        
        # Crear interfaz
        self.create_interface()
        
        # Cargar imagen de fondo si existe
        self.load_background()
    
    def load_config(self) -> dict:
        """Carga configuración del launcher."""
        default_config = {
            "background_image": "",
            "last_selection": "GUI V3",
            "window_size": "800x600"
        }
        
        if self.config_file.exists():
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    return {**default_config, **config}
            except Exception:
                pass
        
        return default_config
    
    def save_config(self):
        """Guarda configuración del launcher."""
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"Error guardando configuración: {e}")
    
    def setup_style(self):
        """Configura el estilo visual del launcher."""
        style = ttk.Style()
        style.theme_use('clam')
        
        # Colores inspirados en la imagen
        bg_color = "#f0f8ff"  # Azul muy claro
        accent_color = "#4a90e2"  # Azul medio
        dark_color = "#2c5aa0"  # Azul oscuro
        
        # Configurar estilos personalizados
        style.configure('Title.TLabel', 
                       font=('Arial', 24, 'bold'),
                       foreground=dark_color,
                       background=bg_color)
        
        style.configure('Subtitle.TLabel',
                       font=('Arial', 12),
                       foreground=accent_color,
                       background=bg_color)
        
        style.configure('Launch.TButton',
                       font=('Arial', 14, 'bold'),
                       padding=(20, 10))
        
        style.configure('Config.TButton',
                       font=('Arial', 10),
                       padding=(10, 5))
        
        self.root.configure(bg=bg_color)
    
    def create_interface(self):
        """Crea la interfaz principal del launcher."""
        # Frame principal con padding
        main_frame = ttk.Frame(self.root, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Título principal
        title_label = ttk.Label(main_frame, 
                               text="🏔️ MUNDO OUTDOOR",
                               style='Title.TLabel')
        title_label.pack(pady=(0, 10))
        
        # Subtítulo
        subtitle_label = ttk.Label(main_frame,
                                  text="Sistema de Gestión de Órdenes MercadoLibre",
                                  style='Subtitle.TLabel')
        subtitle_label.pack(pady=(0, 30))
        
        # Frame central para los controles
        center_frame = ttk.Frame(main_frame)
        center_frame.pack(expand=True, fill=tk.BOTH)
        
        # Espaciador superior
        ttk.Label(center_frame, text="").pack(pady=50)
        
        # Frame para selección de aplicación
        app_frame = ttk.LabelFrame(center_frame, text="📱 Seleccionar Aplicación", padding="20")
        app_frame.pack(pady=20, padx=50, fill=tk.X)
        
        # Dropdown para seleccionar aplicación
        self.app_var = tk.StringVar(value=self.config.get("last_selection", "GUI V3"))
        app_options = [
            "GUI V3 - Interfaz Principal (Recomendado)",
            "GUI V2 - Interfaz Clásica",
        ]
        
        self.app_combo = ttk.Combobox(app_frame, 
                                     textvariable=self.app_var,
                                     values=app_options,
                                     state="readonly",
                                     font=('Arial', 12),
                                     width=40)
        self.app_combo.pack(pady=10)
        
        # Botón de lanzar
        launch_btn = ttk.Button(app_frame,
                               text="🚀 LANZAR APLICACIÓN",
                               command=self.launch_application,
                               style='Launch.TButton')
        launch_btn.pack(pady=20)
        
        # Frame para configuración
        config_frame = ttk.LabelFrame(center_frame, text="⚙️ Configuración", padding="15")
        config_frame.pack(pady=20, padx=50, fill=tk.X)
        
        # Botones de configuración
        btn_frame = ttk.Frame(config_frame)
        btn_frame.pack()
        
        bg_btn = ttk.Button(btn_frame,
                           text="🖼️ Cambiar Fondo",
                           command=self.change_background,
                           style='Config.TButton')
        bg_btn.pack(side=tk.LEFT, padx=5)
        
        reset_btn = ttk.Button(btn_frame,
                              text="🔄 Restablecer",
                              command=self.reset_config,
                              style='Config.TButton')
        reset_btn.pack(side=tk.LEFT, padx=5)
        
        # Frame inferior con información
        info_frame = ttk.Frame(main_frame)
        info_frame.pack(side=tk.BOTTOM, fill=tk.X, pady=10)
        
        info_label = ttk.Label(info_frame,
                              text="💡 Tip: Haz clic en 'Cambiar Fondo' para personalizar con tu imagen PNG o JPG",
                              font=('Arial', 9),
                              foreground="#666666")
        info_label.pack()
        
        # Bind para guardar selección
        self.app_combo.bind('<<ComboboxSelected>>', self.on_selection_change)
    
    def load_background(self):
        """Carga imagen de fondo si existe."""
        bg_path = self.config.get("background_image", "")
        
        # Buscar imagen de fondo automáticamente
        if not bg_path or not Path(bg_path).exists():
            possible_paths = [
                self.base_path / "fondo.png",
                self.base_path / "fondo.jpg", 
                self.base_path / "background.png",
                self.base_path / "background.jpg",
                self.base_path / "logo.png"
            ]
            
            for path in possible_paths:
                if path.exists():
                    bg_path = str(path)
                    break
        
        if bg_path and Path(bg_path).exists():
            try:
                # Cargar imagen con PIL
                pil_image = Image.open(bg_path)
                
                # Redimensionar manteniendo aspecto para llenar toda la ventana
                window_width = 800
                window_height = 600
                pil_image = pil_image.resize((window_width, window_height), Image.Resampling.LANCZOS)
                
                # Aplicar overlay semi-transparente para mejor legibilidad
                overlay = Image.new('RGBA', (window_width, window_height), (0, 0, 0, 100))  # Negro 40% transparente
                pil_image = pil_image.convert('RGBA')
                pil_image = Image.alpha_composite(pil_image, overlay)
                
                # Convertir para Tkinter
                self.bg_image = ImageTk.PhotoImage(pil_image)
                
                # Crear label de fondo
                bg_label = tk.Label(self.root, image=self.bg_image)
                bg_label.place(x=0, y=0, relwidth=1, relheight=1)
                bg_label.lower()  # Enviar al fondo
                
                print(f" Imagen de fondo cargada: {bg_path}")
                
                # Guardar en config
                self.config["background_image"] = bg_path
                self.save_config()
                
            except Exception as e:
                print(f" Error cargando imagen de fondo: {e}")
        else:
            print("")
            self.create_default_background()
    
    def on_window_resize(self, event=None):
        """Maneja el redimensionamiento de la ventana para ajustar el fondo."""
        if hasattr(self, 'bg_canvas') and hasattr(self, 'bg_image'):
            try:
                # Solo redimensionar si el evento es de la ventana principal
                if event and event.widget != self.root:
                    return
                    
                # Recargar imagen con nuevas dimensiones
                bg_path = self.config.get("background_image", "")
                if bg_path and Path(bg_path).exists():
                    img = Image.open(bg_path)
                    new_width = self.root.winfo_width()
                    new_height = self.root.winfo_height()
                    
                    if new_width > 1 and new_height > 1:
                        img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
                        self.bg_image = ImageTk.PhotoImage(img)
                        
                        # Actualizar canvas
                        self.bg_canvas.delete("all")
                        self.bg_canvas.create_image(0, 0, anchor=tk.NW, image=self.bg_image)
                        self.bg_canvas.configure(width=new_width, height=new_height)
            except Exception as e:
                print(f"Error redimensionando fondo: {e}")
    
    def create_default_background(self):
        """Crea un fondo por defecto con gradiente."""
        try:
            # Crear canvas para fondo por defecto
            self.bg_canvas = tk.Canvas(self.root, highlightthickness=0)
            self.bg_canvas.place(x=0, y=0, relwidth=1, relheight=1)
            
            # Crear gradiente simple
            width = 800
            height = 600
            
            # Colores del gradiente (azul claro a azul oscuro)
            for i in range(height):
                # Interpolación de color
                ratio = i / height
                r = int(240 * (1 - ratio) + 44 * ratio)  # De 240 a 44
                g = int(248 * (1 - ratio) + 90 * ratio)   # De 248 a 90
                b = int(255 * (1 - ratio) + 226 * ratio)  # De 255 a 226
                
                color = f"#{r:02x}{g:02x}{b:02x}"
                self.bg_canvas.create_line(0, i, width, i, fill=color, width=1)
            
            # Enviar canvas al fondo
            self.bg_canvas.tk.call('lower', self.bg_canvas._w)
            
        except Exception as e:
            print(f"Error creando fondo por defecto: {e}")
    
    def change_background(self):
        """Permite cambiar la imagen de fondo."""
        file_path = filedialog.askopenfilename(
            title="Seleccionar imagen de fondo",
            filetypes=[
                ("Imágenes", "*.png *.jpg *.jpeg *.gif *.bmp"),
                ("PNG", "*.png"),
                ("JPEG", "*.jpg *.jpeg"),
                ("Todos los archivos", "*.*")
            ]
        )
        
        if file_path:
            self.config["background_image"] = file_path
            self.save_config()
            
            # Recargar fondo
            self.load_background()
            
            messagebox.showinfo("✅ Éxito", 
                              "Imagen de fondo actualizada.\n"
                              "Reinicia el launcher para ver todos los cambios.")
    
    def reset_config(self):
        """Restablece la configuración por defecto."""
        if messagebox.askyesno("🔄 Restablecer", 
                              "¿Estás seguro de que quieres restablecer la configuración?"):
            self.config = {
                "background_image": "",
                "last_selection": "GUI V3",
                "window_size": "800x600"
            }
            self.save_config()
            
            messagebox.showinfo("✅ Éxito", 
                              "Configuración restablecida.\n"
                              "Reinicia el launcher para ver los cambios.")
    
    def on_selection_change(self, event=None):
        """Maneja cambios en la selección de aplicación."""
        selection = self.app_var.get()
        if "V3" in selection:
            self.config["last_selection"] = "GUI V3"
        elif "V2" in selection:
            self.config["last_selection"] = "GUI V2"
        self.save_config()
    
    def launch_application(self):
        """Lanza la aplicación seleccionada."""
        selection = self.app_var.get()
        
        try:
            if "V3" in selection:
                # Lanzar GUI V3
                gui_path = self.base_path / "gui" / "app_gui_v3.py"
                if gui_path.exists():
                    subprocess.Popen([sys.executable, str(gui_path)], 
                                   cwd=str(self.base_path))
                    messagebox.showinfo("🚀 Lanzado", 
                                      "GUI V3 iniciado correctamente.\n"
                                      "Puedes cerrar este launcher si deseas.")
                else:
                    messagebox.showerror("❌ Error", 
                                       f"No se encontró el archivo:\n{gui_path}")
            
            elif "V2" in selection:
                # Lanzar GUI V2
                gui_path = self.base_path / "gui" / "app_gui_v2.py"
                if gui_path.exists():
                    subprocess.Popen([sys.executable, str(gui_path)], 
                                   cwd=str(self.base_path))
                    messagebox.showinfo("🚀 Lanzado", 
                                      "GUI V2 iniciado correctamente.\n"
                                      "Puedes cerrar este launcher si deseas.")
                else:
                    messagebox.showerror("❌ Error", 
                                       f"No se encontró el archivo:\n{gui_path}")
            
        except Exception as e:
            messagebox.showerror("❌ Error", 
                               f"Error al lanzar la aplicación:\n{str(e)}")
    
    def run(self):
        """Ejecuta el launcher."""
        # Centrar ventana
        self.root.update_idletasks()
        x = (self.root.winfo_screenwidth() // 2) - (800 // 2)
        y = (self.root.winfo_screenheight() // 2) - (600 // 2)
        self.root.geometry(f"800x600+{x}+{y}")
        
        # Configurar cierre
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        
        # Iniciar loop principal
        self.root.mainloop()
    
    def on_closing(self):
        """Maneja el cierre del launcher."""
        self.save_config()
        self.root.destroy()

def main():
    """Función principal."""
    try:
        launcher = MundoOutdoorLauncher()
        launcher.run()
    except Exception as e:
        print(f"Error iniciando launcher: {e}")
        input("Presiona Enter para continuar...")

if __name__ == "__main__":
    main()
