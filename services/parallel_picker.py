"""
Procesador paralelo para picking con ventana de progreso visual.
Maneja descuento de stock e impresión de etiqueta en threads separados.
"""

import threading
import queue
import tkinter as tk
from tkinter import ttk
import time
import logging

log = logging.getLogger(__name__)

class ParallelPickProcessor:
    """Procesador que maneja descuento de stock e impresión en paralelo."""
    
    def __init__(self, picker_service):
        self.picker_service = picker_service
        self.progress_window = None
        self.stock_label = None
        self.print_label = None
        self.progress_bar = None
        self.close_button = None
        
        # Queues para comunicación thread-safe
        self.stock_queue = queue.Queue()
        self.print_queue = queue.Queue()
        
        # Estados
        self.stock_completed = False
        self.print_completed = False
        self.stock_success = False
        self.print_success = False
        self.stock_message = ""
        self.print_message = ""
        
    def create_progress_window(self, codigo_ml, pack_id):
        """Crea ventana de progreso con estados visuales."""
        self.progress_window = tk.Toplevel()
        self.progress_window.title("Procesando Picking...")
        self.progress_window.geometry("450x180")
        self.progress_window.resizable(False, False)
        
        # Centrar ventana
        self.progress_window.transient()
        self.progress_window.grab_set()
        
        # Título
        title_label = tk.Label(
            self.progress_window, 
            text=f"📦 {codigo_ml} (Pack: {pack_id})", 
            font=("Arial", 12, "bold")
        )
        title_label.pack(pady=10)
        
        # Frame para status
        status_frame = tk.Frame(self.progress_window)
        status_frame.pack(pady=10, padx=20, fill="x")
        
        # Stock status
        self.stock_label = tk.Label(
            status_frame, 
            text="💰 DESCUENTO STOCK: 🔄 CARGANDO...", 
            anchor="w",
            font=("Arial", 10)
        )
        self.stock_label.pack(fill="x", pady=3)
        
        # Print status  
        self.print_label = tk.Label(
            status_frame, 
            text="🖨️ ETIQUETA: 🔄 CARGANDO...", 
            anchor="w",
            font=("Arial", 10)
        )
        self.print_label.pack(fill="x", pady=3)
        
        # Progress bar general
        self.progress_bar = ttk.Progressbar(
            self.progress_window, 
            mode='indeterminate'
        )
        self.progress_bar.pack(pady=10, padx=20, fill="x")
        self.progress_bar.start()
        
        # Botón cerrar (inicialmente deshabilitado)
        self.close_button = tk.Button(
            self.progress_window,
            text="Cerrar",
            state="disabled",
            command=self.close_window
        )
        self.close_button.pack(pady=5)
        
        return self.progress_window
    
    def update_stock_status(self, status, message=""):
        """Actualiza el estado del descuento de stock (thread-safe)."""
        if self.stock_label:
            self.progress_window.after(0, lambda: self.stock_label.config(
                text=f"💰 DESCUENTO STOCK: {status}",
                fg="green" if "✅" in status else "red" if "❌" in status else "black"
            ))
        self.stock_message = message
    
    def update_print_status(self, status, message=""):
        """Actualiza el estado de la impresión (thread-safe)."""
        if self.print_label:
            self.progress_window.after(0, lambda: self.print_label.config(
                text=f"🖨️ ETIQUETA: {status}",
                fg="green" if "✅" in status else "red" if "❌" in status else "black"
            ))
        self.print_message = message
    
    def stock_worker(self, pack_id, codigo_ml, barcode):
        """Worker thread para descuento de stock."""
        try:
            log.info(f"🧵 Iniciando thread de descuento de stock: {pack_id}")
            self.update_stock_status("🔄 CARGANDO...")
            
            # Simular trabajo (remover en producción)
            time.sleep(0.5)
            
            # Realizar descuento de stock
            ok_stock, msg_stock = self.picker_service.send_stock_movement(
                pack_id, barcode, 1
            )
            
            if ok_stock:
                self.update_stock_status("✅ OK", msg_stock)
                self.stock_success = True
                log.info(f"✅ Stock descontado exitosamente: {pack_id}")
            else:
                self.update_stock_status("❌ FALLO", msg_stock)
                self.stock_success = False
                log.error(f"❌ Fallo descuento de stock: {pack_id} - {msg_stock}")
                
        except Exception as e:
            self.update_stock_status("❌ ERROR", str(e))
            self.stock_success = False
            log.error(f"🔥 Excepción en thread de stock: {pack_id} - {e}", exc_info=True)
        finally:
            self.stock_completed = True
            self.check_completion()
    
    def print_worker(self, shipping_id, pack_id):
        """Worker thread para impresión de etiqueta."""
        try:
            log.info(f"🧵 Iniciando thread de impresión: {shipping_id}")
            self.update_print_status("🔄 CARGANDO...")
            
            # Simular trabajo (remover en producción)
            time.sleep(0.5)
            
            # Realizar impresión
            print_success, print_msg = self.picker_service.print_shipping_label_with_retries(
                shipping_id
            )
            
            if print_success:
                self.update_print_status("✅ OK", print_msg)
                self.print_success = True
                log.info(f"✅ Etiqueta impresa exitosamente: {shipping_id}")
            else:
                self.update_print_status("❌ FALLO", print_msg)
                self.print_success = False
                log.error(f"❌ Fallo impresión etiqueta: {shipping_id} - {print_msg}")
                
        except Exception as e:
            self.update_print_status("❌ ERROR", str(e))
            self.print_success = False
            log.error(f"🔥 Excepción en thread de impresión: {shipping_id} - {e}", exc_info=True)
        finally:
            self.print_completed = True
            self.check_completion()
    
    def check_completion(self):
        """Verifica si ambos threads completaron y habilita cierre."""
        if self.stock_completed and self.print_completed:
            if self.progress_bar:
                self.progress_window.after(0, self.progress_bar.stop)
            if self.close_button:
                self.progress_window.after(0, lambda: self.close_button.config(state="normal"))
            
            # Auto-cerrar después de 3 segundos si ambos fueron exitosos
            if self.stock_success and self.print_success:
                self.progress_window.after(3000, self.close_window)
    
    def close_window(self):
        """Cierra la ventana de progreso."""
        if self.progress_window:
            self.progress_window.grab_release()
            self.progress_window.destroy()
            self.progress_window = None
    
    def process_parallel(self, pack_id, codigo_ml, barcode, shipping_id):
        """
        Procesa descuento de stock e impresión en paralelo.
        
        Args:
            pack_id: ID del pack
            codigo_ml: Código ML del producto
            barcode: Código de barras físico
            shipping_id: ID de envío para impresión
            
        Returns:
            tuple: (stock_success, print_success, stock_msg, print_msg)
        """
        log.info(f"🚀 Iniciando procesamiento paralelo: {pack_id}")
        
        # Resetear estados
        self.stock_completed = False
        self.print_completed = False
        self.stock_success = False
        self.print_success = False
        
        # Crear ventana de progreso
        self.create_progress_window(codigo_ml, pack_id)
        
        # Lanzar threads
        stock_thread = threading.Thread(
            target=self.stock_worker,
            args=(pack_id, codigo_ml, barcode),
            name=f"StockThread-{pack_id}"
        )
        
        print_thread = threading.Thread(
            target=self.print_worker,
            args=(shipping_id, pack_id),
            name=f"PrintThread-{pack_id}"
        )
        
        # Iniciar threads
        stock_thread.daemon = True
        print_thread.daemon = True
        
        stock_thread.start()
        print_thread.start()
        
        log.info(f"🧵 Threads iniciados para {pack_id}")
        
        # La ventana se maneja a sí misma, retornar inmediatamente
        # Los resultados se pueden consultar después si es necesario
        return True, "Procesamiento iniciado en paralelo"
