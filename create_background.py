#!/usr/bin/env python3
"""
Generador de fondo por defecto para MUNDO OUTDOOR Launcher
Crea una imagen de fondo hermosa con tema outdoor/montañas
"""
from PIL import Image, ImageDraw, ImageFont
import math
import os

def create_outdoor_background(width=800, height=600):
    """Crea un fondo hermoso con tema outdoor/montañas para MUNDO OUTDOOR."""
    
    # Crear imagen base con gradiente de cielo
    img = Image.new('RGB', (width, height), '#87CEEB')  # Sky blue
    draw = ImageDraw.Draw(img)
    
    # Crear gradiente de cielo (de azul claro arriba a más oscuro abajo)
    for y in range(height // 2):
        ratio = y / (height // 2)
        r = int(135 + (70 - 135) * ratio)  # De 135 a 70
        g = int(206 + (130 - 206) * ratio)  # De 206 a 130 
        b = int(235 + (180 - 235) * ratio)  # De 235 a 180
        color = f"#{r:02x}{g:02x}{b:02x}"
        draw.line([(0, y), (width, y)], fill=color)
    
    # Colores para las montañas (de fondo a primer plano)
    mountain_colors = [
        '#4682B4',  # Steel blue (montañas lejanas)
        '#2F4F4F',  # Dark slate gray (montañas medias)
        '#228B22',  # Forest green (montañas cercanas)
        '#006400',  # Dark green (primer plano)
    ]
    
    # Crear capas de montañas
    for mountain_idx in range(4):
        mountain_color = mountain_colors[mountain_idx]
        
        # Parámetros de las montañas
        base_height = height * 0.4 + mountain_idx * 40  # Altura base creciente
        peak_variation = 60 - mountain_idx * 10  # Variación de picos decreciente
        
        # Crear puntos de la montaña
        points = []
        num_peaks = 3 + mountain_idx  # Más picos en montañas cercanas
        
        for i in range(num_peaks + 1):
            x = (width * i) // num_peaks
            # Crear picos con variación aleatoria
            peak_height = base_height - peak_variation * (0.5 + 0.5 * math.sin(i * math.pi / 2))
            points.append((x, int(peak_height)))
        
        # Suavizar los picos creando puntos intermedios
        smooth_points = []
        for i in range(len(points) - 1):
            smooth_points.append(points[i])
            # Agregar punto intermedio
            mid_x = (points[i][0] + points[i+1][0]) // 2
            mid_y = (points[i][1] + points[i+1][1]) // 2 + peak_variation // 4
            smooth_points.append((mid_x, mid_y))
        smooth_points.append(points[-1])
        
        # Completar el polígono para rellenar
        smooth_points.extend([(width, height), (0, height)])
        
        # Dibujar montaña con transparencia
        overlay = Image.new('RGBA', (width, height), (0, 0, 0, 0))
        overlay_draw = ImageDraw.Draw(overlay)
        
        # Convertir color hex a RGB
        r = int(mountain_color[1:3], 16)
        g = int(mountain_color[3:5], 16)
        b = int(mountain_color[5:7], 16)
        alpha = 180 - mountain_idx * 30  # Transparencia decreciente
        
        overlay_draw.polygon(smooth_points, fill=(r, g, b, alpha))
        
        # Combinar con la imagen base
        img = Image.alpha_composite(img.convert('RGBA'), overlay).convert('RGB')
    
    # Agregar texto del logo
    try:
        # Intentar usar una fuente más bonita
        try:
            font_large = ImageFont.truetype("arial.ttf", 48)
            font_small = ImageFont.truetype("arial.ttf", 20)
        except:
            # Fallback a fuente por defecto
            font_large = ImageFont.load_default()
            font_small = ImageFont.load_default()
        
        # Texto principal
        text = "🏔️ MUNDO OUTDOOR"
        text_bbox = draw.textbbox((0, 0), text, font=font_large)
        text_width = text_bbox[2] - text_bbox[0]
        text_height = text_bbox[3] - text_bbox[1]
        
        text_x = (width - text_width) // 2
        text_y = 50
        
        # Sombra del texto
        draw.text((text_x + 3, text_y + 3), text, fill='#000000', font=font_large)
        # Texto principal
        draw.text((text_x, text_y), text, fill='#ffffff', font=font_large)
        
        # Subtítulo
        subtitle = "Sistema de Gestión • MercadoLibre"
        subtitle_bbox = draw.textbbox((0, 0), subtitle, font=font_small)
        subtitle_width = subtitle_bbox[2] - subtitle_bbox[0]
        
        subtitle_x = (width - subtitle_width) // 2
        subtitle_y = text_y + text_height + 15
        
        # Sombra del subtítulo
        draw.text((subtitle_x + 2, subtitle_y + 2), subtitle, fill='#000000', font=font_small)
        # Subtítulo
        draw.text((subtitle_x, subtitle_y), subtitle, fill='#ffffff', font=font_small)
        
    except Exception as e:
        print(f"Error agregando texto: {e}")
    
    return img

def main():
    """Genera la imagen de fondo por defecto."""
    print("🎨 Generando imagen de fondo para MUNDO OUTDOOR Launcher...")
    
    # Crear imagen
    background = create_outdoor_background()
    
    # Guardar imagen
    output_path = "mundo_outdoor_background.png"
    background.save(output_path, "PNG", quality=95)
    
    print(f"✅ Imagen de fondo creada: {output_path}")
    print("💡 Esta imagen se usará automáticamente como fondo en el launcher")

if __name__ == "__main__":
    main()
