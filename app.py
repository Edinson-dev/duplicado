"""
app.py - DataCleanse Pro · Enterprise
Versión híbrida: crea carpetas locales + descarga directa de archivos
"""

from flask import Flask, render_template, request, jsonify, session, redirect, url_for, send_file
import pandas as pd
import os, re, glob, traceback, subprocess, platform, io
from datetime import datetime
from functools import wraps
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__)
app.secret_key = "datacleanse-secret-2024"

# ============================================================
# USUARIOS INVITADOS  →  agrega o quita usuarios aquí
# ============================================================
USUARIOS = {
    "admin":    generate_password_hash("admin123"),
    "usuario1": generate_password_hash("clave123"),
    "usuario2": generate_password_hash("clave456"),
}
# ============================================================
# CONFIGURACION
# ============================================================
COLUMNA_FACTURA = "numero_facturado"
COLUMNA_FECHA   = "fecha_prestacion"
EXTENSIONES     = ["*.csv", "*.txt", "*.xlsx", "*.xls", "*.xlsm"]

# Carpeta base: en local usa la carpeta del proyecto, en Railway usa user_data/
ES_RAILWAY = os.environ.get("RAILWAY_ENVIRONMENT") is not None
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
# ============================================================


def carpeta_usuario(username):
    """
    - En Railway: user_data/{usuario}/
    - En local:   carpeta del proyecto (mismo nivel que app.py)
    """
    if ES_RAILWAY:
        path = os.path.join(BASE_DIR, "user_data", username)
    else:
        path = os.path.join(BASE_DIR, username) if username != "admin" else BASE_DIR
    os.makedirs(path, exist_ok=True)
    return path


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "usuario" not in session:
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated


def extraer_contrato(nombre_archivo):
    numeros = re.findall(r'\d{7,}', nombre_archivo)
    return numeros[0] if numeros else os.path.splitext(nombre_archivo)[0]


def limpiar_nombres_columnas(df):
    df.columns = df.columns.str.strip().str.replace('\ufeff', '', regex=False)
    return df


def leer_archivo(ruta):
    _, ext = os.path.splitext(ruta)
    ext = ext.lower()
    if ext in [".xlsx", ".xls", ".xlsm"]:
        return pd.read_excel(ruta)
    df_prueba = pd.read_csv(ruta, nrows=2, header=None, encoding="utf-8-sig")
    if df_prueba.shape[1] == 1:
        return pd.read_csv(ruta, sep=",", encoding="utf-8-sig", low_memory=False)
    return pd.read_csv(ruta, sep=None, engine="python", encoding="utf-8-sig")


def guardar_excel(df, ruta, nombre_hoja):
    nombre_hoja = nombre_hoja[:31]
    with pd.ExcelWriter(ruta, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=nombre_hoja, index=False)
        ws = writer.sheets[nombre_hoja]
        for col in ws.columns:
            valores = [str(c.value) if c.value is not None else "" for c in col[:6]]
            ancho = min(max((len(v) for v in valores), default=10) + 4, 45)
            ws.column_dimensions[col[0].column_letter].width = ancho


def separar_duplicados(df):
    df["_fecha_orden"] = pd.to_datetime(df[COLUMNA_FECHA], errors="coerce", dayfirst=False)
    df["_tiene_fecha"] = df["_fecha_orden"].notna().astype(int)
    df_ord = df.sort_values(
        by=[COLUMNA_FACTURA, "_tiene_fecha", "_fecha_orden"],
        ascending=[True, False, False], na_position="last"
    )
    df_limpio     = df_ord.drop_duplicates(subset=[COLUMNA_FACTURA], keep="first")
    df_duplicados = df_ord[~df_ord.index.isin(df_limpio.index)]
    for frame in [df_limpio, df_duplicados]:
        frame.drop(columns=["_fecha_orden", "_tiene_fecha"], inplace=True)
    return df_limpio.sort_index().reset_index(drop=True), df_duplicados.reset_index(drop=True)


# ═══════════════════════════════════════════════════════════
# AUTH
# ═══════════════════════════════════════════════════════════

@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        user = request.form.get("usuario", "").strip()
        pwd  = request.form.get("clave", "")
        if user in USUARIOS and check_password_hash(USUARIOS[user], pwd):
            session["usuario"] = user
            return redirect(url_for("index"))
        error = "Usuario o contraseña incorrectos"
    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# ═══════════════════════════════════════════════════════════
# PRINCIPALES
# ═══════════════════════════════════════════════════════════

@app.route("/")
@login_required
def index():
    return render_template("index.html", usuario=session["usuario"])


@app.route("/api/listar", methods=["POST"])
@login_required
def listar_archivos():
    carpeta = carpeta_usuario(session["usuario"])
    archivos = []
    for ext in EXTENSIONES:
        archivos.extend(glob.glob(os.path.join(carpeta, ext)))
    # Excluir archivos dentro de subcarpetas Sin Duplicados / Duplicados
    archivos = [
        a for a in archivos
        if not a.endswith(".py")
        and "Sin Duplicados" not in a
        and "Duplicados" not in a
    ]
    archivos_info = [{"nombre": os.path.basename(a), "ruta": a} for a in sorted(archivos)]
    return jsonify({"archivos": archivos_info, "carpeta": carpeta})


@app.route("/api/subir", methods=["POST"])
@login_required
def subir_archivos():
    carpeta  = carpeta_usuario(session["usuario"])
    archivos = request.files.getlist("archivos")
    if not archivos:
        return jsonify({"error": "No se recibieron archivos"}), 400
    ext_validas = {".csv", ".txt", ".xlsx", ".xls", ".xlsm"}
    guardados = []
    for f in archivos:
        _, ext = os.path.splitext(f.filename)
        if ext.lower() not in ext_validas:
            continue
        f.save(os.path.join(carpeta, f.filename))
        guardados.append(f.filename)
    if not guardados:
        return jsonify({"error": "Ningún archivo tiene formato válido"}), 400
    return jsonify({"ok": True, "guardados": guardados, "carpeta": carpeta, "total": len(guardados)})


@app.route("/api/procesar", methods=["POST"])
@login_required
def procesar():
    carpeta  = carpeta_usuario(session["usuario"])
    data     = request.json
    archivos = data.get("archivos", [])
    if not archivos:
        return jsonify({"error": "No se seleccionaron archivos"}), 400

    # ── Crear carpetas de salida ──────────────────────────
    carpeta_limpios    = os.path.join(carpeta, "Sin Duplicados")
    carpeta_duplicados = os.path.join(carpeta, "Duplicados")
    os.makedirs(carpeta_limpios,    exist_ok=True)
    os.makedirs(carpeta_duplicados, exist_ok=True)

    resultados = []

    for ruta_archivo in archivos:
        nombre_archivo = os.path.basename(ruta_archivo)
        nombre_base    = os.path.splitext(nombre_archivo)[0]
        contrato       = extraer_contrato(nombre_archivo)

        try:
            df = leer_archivo(ruta_archivo)
            df = limpiar_nombres_columnas(df)
            filas_orig = len(df)

            for col in [COLUMNA_FACTURA, COLUMNA_FECHA]:
                if col not in df.columns:
                    resultados.append({
                        "archivo": nombre_archivo, "estado": "error",
                        "mensaje": f"No se encontró la columna '{col}'. Columnas: {list(df.columns)}"
                    })
                    break
            else:
                df_limpio, df_duplicados = separar_duplicados(df)

                # Guardar archivo limpio → Sin Duplicados/
                ruta_limpio = os.path.join(carpeta_limpios, f"{nombre_base}.xlsx")
                guardar_excel(df_limpio, ruta_limpio, contrato)

                # Guardar duplicados → Duplicados/{contrato}/
                ruta_dup      = None
                nombre_dup    = None
                if len(df_duplicados) > 0:
                    carpeta_contrato = os.path.join(carpeta_duplicados, contrato)
                    os.makedirs(carpeta_contrato, exist_ok=True)
                    nombre_dup = f"{nombre_base}_duplicados.xlsx"
                    ruta_dup   = os.path.join(carpeta_contrato, nombre_dup)
                    guardar_excel(df_duplicados, ruta_dup, f"{contrato}_dup")

                os.remove(ruta_archivo)

                resultados.append({
                    "archivo":               nombre_archivo,
                    "estado":                "ok",
                    "contrato":              contrato,
                    "filas_originales":      filas_orig,
                    "duplicados_eliminados": len(df_duplicados),
                    "filas_resultado":       len(df_limpio),
                    "ruta_limpio":           ruta_limpio,
                    "ruta_dup":              ruta_dup,
                    "nombre_limpio":         f"{nombre_base}.xlsx",
                    "nombre_dup":            nombre_dup,
                })
                continue

        except Exception as e:
            resultados.append({
                "archivo": nombre_archivo, "estado": "error",
                "mensaje": str(e) + "\n" + traceback.format_exc()
            })

    return jsonify({
        "resultados":         resultados,
        "carpeta_limpios":    carpeta_limpios,
        "carpeta_duplicados": carpeta_duplicados
    })


@app.route("/api/descargar", methods=["POST"])
@login_required
def descargar():
    """Descarga la carpeta de resultados como ZIP."""
    import zipfile, io
    data       = request.json
    tipo       = data.get("tipo", "limpios")
    carpeta    = carpeta_usuario(session["usuario"])
    subcarpeta = os.path.join(carpeta, "Sin Duplicados" if tipo == "limpios" else "Duplicados")

    if not os.path.isdir(subcarpeta):
        return jsonify({"error": "No hay archivos procesados aún"}), 404

    hay_archivos = any(files for _, _, files in os.walk(subcarpeta))
    if not hay_archivos:
        return jsonify({"error": "La carpeta está vacía"}), 404

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(subcarpeta):
            for fname in files:
                full    = os.path.join(root, fname)
                arcname = os.path.relpath(full, subcarpeta)
                zf.write(full, arcname)
    buf.seek(0)

    nombre_zip = f"{session['usuario']}_{tipo}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
    return send_file(buf, as_attachment=True, download_name=nombre_zip,
                     mimetype="application/zip")



@app.route("/api/eliminar", methods=["POST"])
@login_required
def eliminar_archivo():
    """Elimina un archivo subido por el usuario."""
    data    = request.json
    archivo = data.get("archivo", "").strip()
    carpeta = carpeta_usuario(session["usuario"])
    ruta    = os.path.join(carpeta, archivo)

    # Seguridad: verificar que el archivo esté dentro de la carpeta del usuario
    if not os.path.abspath(ruta).startswith(os.path.abspath(carpeta)):
        return jsonify({"error": "Acceso no permitido"}), 403

    if not os.path.isfile(ruta):
        return jsonify({"error": "Archivo no encontrado"}), 404

    os.remove(ruta)
    return jsonify({"ok": True, "eliminado": archivo})

@app.route("/api/abrir-carpeta", methods=["POST"])
@login_required
def abrir_carpeta():
    """Abre carpeta en el explorador — solo funciona en versión local."""
    data       = request.json
    tipo       = data.get("tipo", "limpios")
    carpeta    = carpeta_usuario(session["usuario"])
    subcarpeta = os.path.join(carpeta, "Sin Duplicados" if tipo == "limpios" else "Duplicados")

    if not os.path.isdir(subcarpeta):
        return jsonify({"error": "Carpeta no encontrada"}), 404

    sistema = platform.system()
    if sistema == "Linux" and not os.environ.get("DISPLAY"):
        return jsonify({"local": False, "ruta": subcarpeta}), 200

    try:
        if sistema == "Windows":
            subprocess.Popen(["explorer", subcarpeta])
        elif sistema == "Darwin":
            subprocess.Popen(["open", subcarpeta])
        else:
            subprocess.Popen(["xdg-open", subcarpeta])
        return jsonify({"ok": True, "local": True})
    except Exception as e:
        return jsonify({"local": False, "ruta": subcarpeta}), 200


if __name__ == "__main__":
    print("\n" + "="*55)
    print("  DataCleanse Pro · Modo Web Compartido")
    print("="*55)
    print("  Servidor iniciando...")
    print("="*55 + "\n")

    port = int(os.environ.get("PORT", 8000))
    app.run(debug=False, host="0.0.0.0", port=port)
