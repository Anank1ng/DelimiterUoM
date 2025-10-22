# app.py
# Streamlit app to clean and convert uploaded CSV/Excel files to a standardized
# comma-delimited CSV (UTF-8), quoting all fields, with options for date/number normalization.
# Designed for Oracle Fusion UOM Interclass Conversion uploads, but generic enough for other datasets.

import csv
import io
import re
from datetime import datetime
from typing import List, Optional

import pandas as pd
import streamlit as st

# =============================
# Utility functions
# =============================

def sniff_delimiter(sample: str) -> str:
    """Try to detect delimiter from a text sample."""
    try:
        dialect = csv.Sniffer().sniff(sample)
        return dialect.delimiter
    except Exception:
        # Heuristics: prefer semicolon, then tab, else comma
        head = sample.splitlines()[0] if sample else ""
        if ";" in head:
            return ";"
        if "\t" in head:
            return "\t"
        return ","


def strip_bom(colname: str) -> str:
    return colname.lstrip("\ufeff") if colname else colname


def trim_df(df: pd.DataFrame) -> pd.DataFrame:
    return df.applymap(lambda x: x.strip() if isinstance(x, str) else x)


def remove_duplicate_header_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    header = list(df.columns)
    header_set = set([str(h).strip() for h in header])

    def is_header_row(row) -> bool:
        vals = set([str(v).strip() for v in row.values.tolist()])
        return vals == header_set

    mask = df.apply(is_header_row, axis=1)
    if mask.any():
        df = df[~mask].copy()
    return df


def normalize_dates(df: pd.DataFrame, date_cols: List[str], fmt: str = "yyyy-mm-dd") -> pd.DataFrame:
    # Map Excel-like fmt to Python strftime
    fmt_map = {
        "yyyy-mm-dd": "%Y-%m-%d",
        "dd/mm/yyyy": "%d/%m/%Y",
        "mm/dd/yyyy": "%m/%d/%Y",
    }
    target = fmt_map.get(fmt.lower(), "%Y-%m-%d")

    def parse_any(s: str) -> Optional[pd.Timestamp]:
        s = s.strip()
        if not s:
            return None
        # Try fixed formats first
        fmts = ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%m-%d-%Y", "%d %b %Y", "%d %B %Y"]
        for f in fmts:
            try:
                return pd.to_datetime(datetime.strptime(s, f))
            except Exception:
                pass
        # Fall back to pandas parser (dayfirst heuristic)
        ts = pd.to_datetime(s, dayfirst=True, errors="coerce")
        return ts if not pd.isna(ts) else None

    for c in date_cols:
        if c not in df.columns:
            continue
        df[c] = df[c].map(lambda v: "" if pd.isna(v) or str(v).strip()=="" else (
            parse_any(str(v)).strftime(target) if parse_any(str(v)) else str(v).strip()
        ))
    return df


def normalize_numbers(df: pd.DataFrame, num_cols: List[str], decimal_point: str = ".") -> pd.DataFrame:
    # Convert comma decimals to dot if requested
    for c in num_cols:
        if c not in df.columns:
            continue
        def fix_num(x):
            if x is None or (isinstance(x, float) and pd.isna(x)):
                return ""
            s = str(x).strip()
            # Replace decimal comma with dot for numeric-like tokens
            if decimal_point == "." and re.match(r"^-?\d+,\d+$", s):
                s = s.replace(",", ".")
            return s
        df[c] = df[c].map(fix_num)
    return df


def write_csv_download(df: pd.DataFrame, quote_all: bool = True) -> bytes:
    output = io.StringIO()
    writer = csv.writer(output, delimiter=",", quoting=csv.QUOTE_ALL if quote_all else csv.QUOTE_MINIMAL, lineterminator="\n")
    writer.writerow(list(df.columns))
    for _, row in df.iterrows():
        writer.writerow([("" if (isinstance(v, float) and pd.isna(v)) else ("" if v is None else str(v))) for v in row.tolist()])
    data = output.getvalue().encode("utf-8")
    return data


# =============================
# Streamlit UI
# =============================

st.set_page_config(page_title="CSV Cleaner & Converter (UOM Interclass)", page_icon="🧼", layout="wide")

st.title("🧼 CSV Cleaner & Converter")
st.caption("Upload file CSV/Excel, rapikan delimiter & formatnya, lalu unduh kembali siap upload ke Oracle.")

with st.sidebar:
    st.header("Pengaturan")
    date_format = st.selectbox("Format tanggal output", ["yyyy-mm-dd", "dd/mm/yyyy", "mm/dd/yyyy"], index=0)
    quote_all = st.checkbox("Quote semua kolom", value=True, help="Disarankan ON untuk aman terhadap koma/newline dalam data.")

    st.subheader("Deteksi Kolom Otomatis")
    auto_date_cols = st.checkbox("Deteksi kolom tanggal otomatis (berisi 'DATE')", value=True)
    auto_num_cols = st.checkbox("Deteksi kolom angka otomatis (berisi 'RATE' atau 'CONVERSION')", value=True)

    st.subheader("Penyesuaian Manual (opsional)")
    custom_date_cols = st.text_input("Kolom tanggal manual (pisahkan koma)", value="")
    custom_num_cols = st.text_input("Kolom angka manual (pisahkan koma)", value="")

uploaded = st.file_uploader("Pilih file (.csv, .xls, .xlsx)", type=["csv", "xls", "xlsx"], accept_multiple_files=False)

if uploaded:
    # Read raw bytes for CSV sniffing
    raw_bytes = uploaded.read()
    uploaded.seek(0)

    df: Optional[pd.DataFrame] = None
    source_info = ""

    try:
        if uploaded.name.lower().endswith(".csv"):
            text = raw_bytes.decode("utf-8", errors="replace")
            delim = sniff_delimiter(text[:2000])
            df = pd.read_csv(io.StringIO(text), sep=delim, dtype=str, engine="python")
            source_info = f"CSV terdeteksi dengan delimiter: `{delim}`"
        else:
            # Excel path
            xl = pd.ExcelFile(io.BytesIO(raw_bytes))
            first_sheet = xl.sheet_names[0]
            df = xl.parse(first_sheet, dtype=str)
            source_info = f"Excel terbaca. Sheet: `{first_sheet}`"
    except Exception as e:
        st.error(f"Gagal membaca file: {e}")

    if df is not None:
        # Clean up
        df.columns = [strip_bom(c) for c in df.columns]
        df = trim_df(df)
        # Drop entirely empty columns
        empty_cols = [c for c in df.columns if df[c].isna().all()]
        if empty_cols:
            df = df.drop(columns=empty_cols)
        df = remove_duplicate_header_rows(df)

        # Detect columns
        date_cols = [c for c in df.columns if ("DATE" in c.upper())] if auto_date_cols else []
        if custom_date_cols.strip():
            date_cols += [c.strip() for c in custom_date_cols.split(",") if c.strip() in df.columns]
        date_cols = list(dict.fromkeys(date_cols))  # dedupe keep order

        num_cols = [c for c in df.columns if ("CONVERSION" in c.upper() or "RATE" in c.upper())] if auto_num_cols else []
        if custom_num_cols.strip():
            num_cols += [c.strip() for c in custom_num_cols.split(",") if c.strip() in df.columns]
        num_cols = list(dict.fromkeys(num_cols))

        # Normalize
        df = normalize_dates(df, date_cols, fmt=date_format)
        df = normalize_numbers(df, num_cols, decimal_point=".")

        st.success(source_info)
        st.write("**Ringkasan kolom terdeteksi**")
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Jumlah kolom", len(df.columns))
            st.write("Tanggal:", ", ".join(date_cols) if date_cols else "(tidak terdeteksi)")
        with col2:
            st.metric("Jumlah baris", len(df))
            st.write("Angka (rate/konversi):", ", ".join(num_cols) if num_cols else "(tidak terdeteksi)")

        st.divider()
        st.subheader("Preview data")
        st.dataframe(df.head(50), use_container_width=True)

        # Build downloadable CSV
        cleaned_csv = write_csv_download(df, quote_all=quote_all)
        st.download_button(
            label="⬇️ Unduh CSV Bersih (UTF-8, comma-delimited)",
            data=cleaned_csv,
            file_name=(uploaded.name.rsplit(".",1)[0] + "_clean.csv"),
            mime="text/csv",
        )

        with st.expander("Catatan & Tips"):
            st.markdown(
                """
                - Baris header duplikat otomatis dihapus (jika ditemukan di tengah data).
                - Kolom kosong penuh dibuang (berasal dari trailing delimiter).
                - Semua nilai di-*quote* agar aman terhadap koma/newline.
                - Tanggal dinormalisasi ke format pilihan; jika sebuah nilai tidak terbaca sebagai tanggal, nilai asli dipertahankan.
                - Angka dengan desimal koma (mis. `1,25`) diubah menjadi titik (`1.25`) pada kolom angka yang terdeteksi/ditentukan.
                - Cocok untuk load **Unit of Measure – Interclass Conversion** di Oracle.
                """
            )

else:
    st.info("Unggah file CSV/Excel untuk mulai memproses.")
