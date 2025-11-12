import streamlit as st
import pandas as pd
import numpy as np
import re
import matplotlib.pyplot as plt
import seaborn as sns
import glob 
import os   

# Atur konfigurasi halaman
st.set_page_config(layout="wide", page_title="Komparasi Data OTA Bus")

# --- PATH FOLDER DATA ---
REDBUS_PATH = 'data/redbus'
TRAVELOKA_PATH = 'data/traveloka'

# --- FUNGSI PEMBERSHAN DATA ---
def clean_single_dataframe(df, ota_name):
    """Menerapkan logika pembersihan pada satu DataFrame."""
    
    if 'Route_Link' in df.columns:
        df = df.drop('Route_Link', axis=1)

    # 1. Pembersihan dan Penyeragaman Harga
    df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
    df.dropna(subset=['Price'], inplace=True)
    df['Price'] = df['Price'].astype(int)

    # 2. Fungsi untuk Menyeragamkan Durasi ke Menit
    def convert_duration_to_minutes(duration_str):
        if isinstance(duration_str, str):
            hours, minutes = 0, 0
            hour_match = re.search(r'(\d+)[hj]', duration_str) 
            if hour_match:
                hours = int(hour_match.group(1))
            minute_match = re.search(r'(\d+)m', duration_str)
            if minute_match:
                minutes = int(minute_match.group(1))
            return (hours * 60) + minutes
        return None

    df['Duration_Minutes'] = df['Duration'].apply(convert_duration_to_minutes)
    
    # 3. Standardisasi Nama Bus dan Tipe Bus
    df['Bus_Name'] = df['Bus_Name'].str.strip()
    df['Bus_Type'] = df['Bus_Type'].str.strip()
    
    # 4. Tambahkan Identifier OTA
    df['OTA'] = ota_name
    
    # 5. Ekstraksi Kursi Tersedia
    if ota_name == 'Redbus' and 'Seat_Availability' in df.columns:
        # Pembersihan kursi Redbus dari teks/spasi (\xa0)
        df['Seats_Available'] = df['Seat_Availability'].str.replace(r'[^0-9]+', ' ', regex=True).str.strip().str.split().str[0]
        df['Seats_Available'] = pd.to_numeric(df['Seats_Available'], errors='coerce').fillna(0).astype(int)
    
    elif ota_name == 'Traveloka' and 'Seats' in df.columns:
        # Ekstraksi kursi Traveloka
        df['Seats_Available'] = pd.to_numeric(df['Seats'], errors='coerce').fillna(0).astype(int)
    else:
        df['Seats_Available'] = 0
        
    return df

# --- FUNGSI PEMUATAN DARI FOLDER ---
@st.cache_data
def load_data_from_folder(folder_path, ota_name):
    """Mencari, memuat, dan menggabungkan semua CSV dari folder."""
    all_files = glob.glob(os.path.join(folder_path, "*.csv"))
    
    if not all_files:
        return pd.DataFrame(), 0
    
    list_df = []
    
    for filename in all_files:
        try:
            df = pd.read_csv(filename)
            df_clean = clean_single_dataframe(df, ota_name)
            list_df.append(df_clean)
        except Exception as e:
            pass 

    if not list_df:
        return pd.DataFrame(), len(all_files)
        
    combined_df = pd.concat(list_df, ignore_index=True)
    
    return combined_df, len(all_files)

# --- FUNGSI ANALISIS LAYANAN PER OPERATOR (Tanpa Rute - Global) ---
@st.cache_data
def analyze_operator_services(df_redbus, df_traveloka):
    """Menggabungkan data dan menganalisis Bus_Type, serta harga MIN, MAX, dan AVG per Bus_Name."""
    
    df_combined = pd.concat([df_redbus, df_traveloka], ignore_index=True)
    
    service_price = df_combined.groupby(['Bus_Name', 'Bus_Type'])['Price'].agg(
        ['min', 'max', 'mean']
    ).reset_index()
    
    service_price.rename(columns={'min': 'Harga Min (IDR)', 'max': 'Harga Max (IDR)', 'mean': 'Harga Rata-rata (IDR)'}, inplace=True)
    service_price = service_price.sort_values(by=['Bus_Name', 'Harga Rata-rata (IDR)'], ascending=[True, True])
    service_price['Harga Rata-rata (IDR)'] = service_price['Harga Rata-rata (IDR)'].astype(int)
    service_price['Harga Min (IDR)'] = service_price['Harga Min (IDR)'].astype(int)
    service_price['Harga Max (IDR)'] = service_price['Harga Max (IDR)'].astype(int)

    return service_price

# --- FUNGSI KOMPARASI JADWAL DETAIL ---
@st.cache_data
def compare_detailed_schedules(df_redbus, df_traveloka):
    """
    Melakukan outer join setelah mengelompokkan data berdasarkan Tanggal, Jam, PO, dan Tipe Bus.
    Mengambil Min Price, Max Seats, Min Duration.
    """
    
    # Keys untuk pengelompokan dan penggabungan
    merge_keys = ['Route_Date', 'Departing_Time', 'Bus_Name', 'Bus_Type']
    
    # Agregasi: Harga Min, Kursi Max, Durasi Min
    agg_map = {
        'Price': 'min',
        'Seats_Available': 'max',
        'Duration_Minutes': 'min',
        'Duration': 'first', # Ambil salah satu string Durasi untuk display
    }
    
    # 1. Agregasi data Redbus
    df_r_agg = df_redbus.groupby(merge_keys).agg(agg_map).reset_index()
    df_r_agg.rename(columns={
        'Price': 'Price_Redbus', 
        'Seats_Available': 'Seats_Available_Redbus',
        'Duration_Minutes': 'Duration_Minutes_Redbus',
        'Duration': 'Duration_Redbus',
    }, inplace=True)

    # 2. Agregasi data Traveloka
    df_t_agg = df_traveloka.groupby(merge_keys).agg(agg_map).reset_index()
    df_t_agg.rename(columns={
        'Price': 'Price_Traveloka', 
        'Seats_Available': 'Seats_Available_Traveloka',
        'Duration_Minutes': 'Duration_Minutes_Traveloka',
        'Duration': 'Duration_Traveloka',
    }, inplace=True)
    
    # 3. Lakukan Outer Merge pada jadwal yang sudah unik
    comparison_detail = pd.merge(
        df_r_agg, df_t_agg, on=merge_keys, how='outer'
    )

    # Isi nilai yang hilang dan konversi tipe data
    comparison_detail['Price_Redbus'] = comparison_detail['Price_Redbus'].fillna(0).astype(int)
    comparison_detail['Price_Traveloka'] = comparison_detail['Price_Traveloka'].fillna(0).astype(int)
    
    comparison_detail['Duration_Minutes_Redbus'] = comparison_detail['Duration_Minutes_Redbus'].fillna(0).astype(int)
    comparison_detail['Duration_Minutes_Traveloka'] = comparison_detail['Duration_Minutes_Traveloka'].fillna(0).astype(int)
    
    comparison_detail['Seats_Available_Redbus'] = comparison_detail['Seats_Available_Redbus'].fillna(0).astype(int)
    comparison_detail['Seats_Available_Traveloka'] = comparison_detail['Seats_Available_Traveloka'].fillna(0).astype(int)
    
    # Hitung selisih harga dan status komparasi
    comparison_detail['Selisih Harga'] = (comparison_detail['Price_Redbus'] - comparison_detail['Price_Traveloka']).abs().astype(int)
    
    conditions = [
        (comparison_detail['Price_Redbus'] > 0) & (comparison_detail['Price_Traveloka'] == 0),
        (comparison_detail['Price_Redbus'] == 0) & (comparison_detail['Price_Traveloka'] > 0),
        (comparison_detail['Price_Redbus'] < comparison_detail['Price_Traveloka']) & (comparison_detail['Price_Redbus'] > 0),
        (comparison_detail['Price_Redbus'] > comparison_detail['Price_Traveloka']) & (comparison_detail['Price_Traveloka'] > 0),
    ]
    choices = ['Hanya Redbus', 'Hanya Traveloka', 'Redbus Lebih Murah', 'Traveloka Lebih Murah']
    comparison_detail['Status Komparasi'] = np.select(conditions, choices, default='Harga Sama / Tidak Ada')

    comparison_detail = comparison_detail.sort_values(by=['Route_Date', 'Departing_Time', 'Bus_Name'])
    
    return comparison_detail

# --- FUNGSI KOMPARASI HARGA PER RUTE, PO, DAN TIPE ---
@st.cache_data
def analyze_service_price_by_route(df_redbus, df_traveloka):
    """
    Membandingkan Harga Rata-rata per Rute, PO Bus, dan Tipe Bus di kedua OTA.
    """
    
    group_keys = ['Route_Name', 'Bus_Name', 'Bus_Type']
    
    # 1. Harga Rata-rata Redbus
    avg_price_redbus = df_redbus.groupby(group_keys)['Price'].mean().reset_index().rename(columns={'Price': 'Price_redbus'})
    
    # 2. Harga Rata-rata Traveloka
    avg_price_traveloka = df_traveloka.groupby(group_keys)['Price'].mean().reset_index().rename(columns={'Price': 'Price_traveloka'})

    # 3. Gabungkan hasilnya
    comparison_df = pd.merge(
        avg_price_redbus, 
        avg_price_traveloka, 
        on=group_keys, 
        how='outer'
    )
    
    # 4. Pembersihan dan Perhitungan Status
    comparison_df['Harga Redbus (Rata-rata)'] = comparison_df['Price_redbus'].fillna(0).astype(int)
    comparison_df['Harga Traveloka (Rata-rata)'] = comparison_df['Price_traveloka'].fillna(0).astype(int)
    comparison_df.drop(columns=['Price_redbus', 'Price_traveloka'], inplace=True)
    
    comparison_df['Selisih Harga'] = (comparison_df['Harga Redbus (Rata-rata)'] - comparison_df['Harga Traveloka (Rata-rata)']).abs().astype(int)
    
    conditions = [
        comparison_df['Harga Redbus (Rata-rata)'] < comparison_df['Harga Traveloka (Rata-rata)'],
        comparison_df['Harga Redbus (Rata-rata)'] > comparison_df['Harga Traveloka (Rata-rata)'],
        (comparison_df['Harga Redbus (Rata-rata)'] == 0) & (comparison_df['Harga Traveloka (Rata-rata)'] > 0), 
        (comparison_df['Harga Redbus (Rata-rata)'] > 0) & (comparison_df['Harga Traveloka (Rata-rata)'] == 0), 
    ]
    choices = ['Redbus Lebih Murah', 'Traveloka Lebih Murah', 'Hanya Traveloka', 'Hanya Redbus']
    comparison_df['Status Harga'] = np.select(conditions, choices, default='Harga Sama / Tidak Ada')

    comparison_df = comparison_df.rename(columns={
        'Route_Name': 'Rute',
        'Bus_Name': 'PO Bus',
        'Bus_Type': 'Tipe Bus'
    })
    
    comparison_df = comparison_df[['Rute', 'PO Bus', 'Tipe Bus', 'Harga Redbus (Rata-rata)', 'Harga Traveloka (Rata-rata)', 'Selisih Harga', 'Status Harga']]
    comparison_df = comparison_df.sort_values(by=['Rute', 'PO Bus', 'Tipe Bus'])
    
    return comparison_df

# --- FUNGSI KOMPARASI HARGA RATA-RATA OPERATOR (Global - Lama) ---
@st.cache_data
def perform_comparison(df_redbus, df_traveloka):
    """Membandingkan Harga Rata-rata Operator di seluruh rute (lama Section 4/baru Section 5)."""
    avg_price_redbus = df_redbus.groupby(['Bus_Name', 'Bus_Type'])['Price'].mean().reset_index()
    avg_price_traveloka = df_traveloka.groupby(['Bus_Name', 'Bus_Type'])['Price'].mean().reset_index()

    comparison_df = pd.merge(avg_price_redbus, avg_price_traveloka, on=['Bus_Name', 'Bus_Type'], how='outer', suffixes=('_redbus', '_traveloka'))
    
    comparison_df['Harga Redbus'] = comparison_df['Price_redbus'].fillna(0).astype(int)
    comparison_df['Harga Traveloka'] = comparison_df['Price_traveloka'].fillna(0).astype(int)
    comparison_df.drop(columns=['Price_redbus', 'Price_traveloka'], inplace=True)
    
    comparison_df['Selisih Harga'] = (comparison_df['Harga Redbus'] - comparison_df['Harga Traveloka']).abs().astype(int)
    
    conditions = [
        comparison_df['Harga Redbus'] < comparison_df['Harga Traveloka'],
        comparison_df['Harga Redbus'] > comparison_df['Harga Traveloka'],
        (comparison_df['Harga Redbus'] == 0) & (comparison_df['Harga Traveloka'] > 0), 
        (comparison_df['Harga Redbus'] > 0) & (comparison_df['Harga Traveloka'] == 0), 
    ]
    choices = ['Redbus Lebih Murah', 'Traveloka Lebih Murah', 'Hanya Traveloka', 'Hanya Redbus']
    comparison_df['Status Harga'] = np.select(conditions, choices, default='Harga Sama / Tidak Ada')

    comparison_df = comparison_df.sort_values(by=['Status Harga', 'Selisih Harga'], ascending=[True, False])
    return comparison_df

def extract_route_metadata(df_clean, ota_name, file_count):
    if df_clean.empty:
        return f"**{ota_name}**: (0 file dimuat. Data kosong atau gagal diproses.)"
        
    route_names = df_clean['Route_Name'].unique() if 'Route_Name' in df_clean.columns else ["Tidak Diketahui"]
    route_dates = sorted(df_clean['Route_Date'].unique()) if 'Route_Date' in df_clean.columns else ["Tidak Diketahui"]

    route_display = f"{len(route_names)} Rute Unik: {', '.join(route_names[:3])}{'...' if len(route_names) > 3 else ''}"
    
    if len(route_dates) > 1:
        date_display = f"Rentang Tanggal: {route_dates[0]} sampai {route_dates[-1]}"
    else:
        date_display = f"Tanggal: {route_dates[0]}"
    
    return f"**{ota_name}** ({file_count} file, {len(df_clean)} baris data): {route_display}, **{date_display}**"

# Fungsi pembantu untuk konversi menit ke format HHj MMm
def minutes_to_duration(minutes):
    if minutes == 0:
        return "-"
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours}j {mins}m"


# =================================================================
#                         TATA LETAK WEB
# =================================================================

st.title("🚌 Komparasi Data OTA Bus (Akses Folder Otomatis)")
st.markdown("Aplikasi ini secara otomatis memuat semua file CSV dari folder: `data/redbus` dan `data/traveloka`.")

# --- SIDEBAR: Status Folder ---
st.sidebar.header("Status Folder Data")
st.sidebar.markdown(f"Path Redbus: `{REDBUS_PATH}`")
st.sidebar.markdown(f"Path Traveloka: `{TRAVELOKA_PATH}`")

# --- Pengecekan Folder ---
if not os.path.isdir('data') or not os.path.isdir(REDBUS_PATH) or not os.path.isdir(TRAVELOKA_PATH):
    st.error(f"""
        ❌ **Error Fatal: Folder tidak ditemukan!**
    """)
else:
    # --- Pemuatan Data dari Folder ---
    with st.spinner('Memuat, menggabungkan, dan membersihkan data dari folder...'):
        df_traveloka_clean, count_t = load_data_from_folder(TRAVELOKA_PATH, 'Traveloka')
        df_redbus_clean, count_r = load_data_from_folder(REDBUS_PATH, 'Redbus')

    if df_traveloka_clean.empty and df_redbus_clean.empty:
        st.error(f"⚠️ **Gagal memuat data yang cukup.** Ditemukan 0 baris data valid dari kedua folder.")
    elif df_traveloka_clean.empty or df_redbus_clean.empty:
        st.warning(f"⚠️ **Hanya data dari satu platform yang ditemukan.**")
    
    if not df_traveloka_clean.empty or not df_redbus_clean.empty:
        
        # --- Gabungan data mentah (digunakan untuk Summary Section 4) ---
        df_combined = pd.concat([df_redbus_clean, df_traveloka_clean], ignore_index=True)
        df_all_po = df_combined['Bus_Name'].dropna().unique()
        
        # -------------------------------------------------------------
        # 1. KETERANGAN RUTE DAN TANGGAL GABUNGAN
        # -------------------------------------------------------------
        route_info_t = extract_route_metadata(df_traveloka_clean, 'Traveloka', count_t)
        route_info_r = extract_route_metadata(df_redbus_clean, 'Redbus', count_r)
        
        st.header("1. Keterangan Data yang Dibandingkan")
        st.info(f"""
            **Analisis Komparasi Dilakukan Berdasarkan Data Gabungan dari Folder:**
            - {route_info_t}
            - {route_info_r}
        """)

        # -------------------------------------------------------------
        # 2. ANALISIS LAYANAN PER OPERATOR (MIN, MAX, AVG)
        # -------------------------------------------------------------
        st.header("2. Analisis Layanan Bus per Operator (Gabungan Data)")
        st.markdown("Tabel ini menunjukkan tipe layanan, harga **minimum**, **maksimum**, dan **rata-rata** yang ditawarkan oleh setiap operator di **seluruh** data yang digabungkan.")
        
        service_analysis_df = analyze_operator_services(df_redbus_clean, df_traveloka_clean)
        
        # --- WIDGET FILTER ---
        all_bus_names = service_analysis_df['Bus_Name'].unique()
        all_bus_types = service_analysis_df['Bus_Type'].unique()
        
        col_filter1, col_filter2 = st.columns(2)
        
        with col_filter1:
            selected_names = st.multiselect("Filter berdasarkan PO Bus:", options=all_bus_names, default=[])

        with col_filter2:
            selected_types = st.multiselect("Filter berdasarkan Tipe Bus:", options=all_bus_types, default=[])

        # --- APLIKASI FILTER ---
        filtered_service_df = service_analysis_df.copy()
        if selected_names:
            filtered_service_df = filtered_service_df[filtered_service_df['Bus_Name'].isin(selected_names)]
        if selected_types:
            filtered_service_df = filtered_service_df[filtered_service_df['Bus_Type'].isin(selected_types)]
        
        
        if filtered_service_df.empty:
            st.warning("Tidak ada data yang cocok dengan kriteria filter yang dipilih.")
        else:
            styled_df = filtered_service_df.copy()
            styled_df['Bus_Name_Display'] = styled_df['Bus_Name'].where(
                styled_df['Bus_Name'] != styled_df['Bus_Name'].shift(1), ''
            )
            
            st.dataframe(
                styled_df[[
                    'Bus_Name_Display', 'Bus_Type', 'Harga Min (IDR)', 'Harga Max (IDR)', 'Harga Rata-rata (IDR)'
                ]].rename(columns={'Bus_Name_Display': 'Bus_Name', 'Bus_Type': 'Tipe Bus'}), 
                use_container_width=True
            )

        # -------------------------------------------------------------
        # 3. KOMPARASI DETAIL JADWAL & KETERSEDIAAN
        # -------------------------------------------------------------
        st.header("3. Komparasi Detail Jadwal & Ketersediaan")
        st.markdown("Tabel ini menunjukkan perbandingan terperinci yang **sudah dikelompokkan** berdasarkan **Tanggal, Jam, PO Bus, dan Tipe Bus** (mengambil Harga Min, Kursi Max, dan Durasi Min).")
        
        detail_comparison_df = compare_detailed_schedules(df_redbus_clean, df_traveloka_clean)
        
        # --- Filter Tanggal di Bagian 3 ---
        all_dates = detail_comparison_df['Route_Date'].unique()
        selected_date = st.selectbox("Filter Jadwal Berdasarkan Tanggal:", options=all_dates)
        
        filtered_detail_df = detail_comparison_df.copy()
        if selected_date:
            filtered_detail_df = filtered_detail_df[filtered_detail_df['Route_Date'] == selected_date]

        # --- HITUNG STATISTIK HARGA & DURASI ---
        if not filtered_detail_df.empty:
            
            price_redbus_valid = filtered_detail_df[filtered_detail_df['Price_Redbus'] > 0]
            price_traveloka_valid = filtered_detail_df[filtered_detail_df['Price_Traveloka'] > 0]
            duration_redbus_valid = filtered_detail_df[filtered_detail_df['Duration_Minutes_Redbus'] > 0]
            duration_traveloka_valid = filtered_detail_df[filtered_detail_df['Duration_Minutes_Traveloka'] > 0]
            
            # Perhitungan Durasi Aman (Menggunakan np.nan_to_num)
            mean_r_safe = int(np.nan_to_num(duration_redbus_valid['Duration_Minutes_Redbus'].mean()))
            min_r_safe = int(np.nan_to_num(duration_redbus_valid['Duration_Minutes_Redbus'].min()))
            max_r_safe = int(np.nan_to_num(duration_redbus_valid['Duration_Minutes_Redbus'].max()))

            mean_t_safe = int(np.nan_to_num(duration_traveloka_valid['Duration_Minutes_Traveloka'].mean()))
            min_t_safe = int(np.nan_to_num(duration_traveloka_valid['Duration_Minutes_Traveloka'].min()))
            max_t_safe = int(np.nan_to_num(duration_traveloka_valid['Duration_Minutes_Traveloka'].max()))

            # 1. Harga
            stat_harga = pd.DataFrame({
                'Statistik': ['Harga Min (IDR)', 'Harga Max (IDR)', 'Harga Rata-rata (IDR)'],
                'Redbus': [
                    price_redbus_valid['Price_Redbus'].min(), 
                    price_redbus_valid['Price_Redbus'].max(), 
                    price_redbus_valid['Price_Redbus'].mean()
                ],
                'Traveloka': [
                    price_traveloka_valid['Price_Traveloka'].min(), 
                    price_traveloka_valid['Price_Traveloka'].max(), 
                    price_traveloka_valid['Price_Traveloka'].mean()
                ]
            }).fillna(0).astype({'Redbus': int, 'Traveloka': int})
            
            # 2. Durasi (Menggunakan hasil perhitungan aman)
            stat_durasi = pd.DataFrame({
                'Statistik': ['Durasi Min', 'Durasi Max', 'Durasi Rata-rata'],
                'Redbus': [
                    minutes_to_duration(min_r_safe),
                    minutes_to_duration(max_r_safe),
                    minutes_to_duration(mean_r_safe)
                ],
                'Traveloka': [
                    minutes_to_duration(min_t_safe),
                    minutes_to_duration(max_t_safe),
                    minutes_to_duration(mean_t_safe)
                ]
            }).fillna("-")

            st.subheader(f"Ringkasan Statistik untuk Tanggal: {selected_date}")
            col_stat1, col_stat2 = st.columns(2)
            
            with col_stat1:
                st.markdown("**Statistik Harga**")
                st.dataframe(stat_harga, use_container_width=True, hide_index=True)

            with col_stat2:
                st.markdown("**Statistik Durasi Perjalanan**")
                st.dataframe(stat_durasi, use_container_width=True, hide_index=True)
            
            st.markdown("---") 
            
            # Tampilkan tabel detail
            st.dataframe(
                filtered_detail_df[[
                    'Route_Date', 'Departing_Time', 'Bus_Name', 'Bus_Type', 'Status Komparasi',
                    'Price_Redbus', 'Price_Traveloka', 'Selisih Harga', 
                    'Seats_Available_Redbus', 'Seats_Available_Traveloka', 
                    'Duration_Redbus', 'Duration_Traveloka'
                ]].rename(
                    columns={
                        'Price_Redbus': 'Harga Redbus (Min)', 'Price_Traveloka': 'Harga Traveloka (Min)',
                        'Seats_Available_Redbus': 'Kursi Redbus (Max)', 'Seats_Available_Traveloka': 'Kursi Traveloka (Max)',
                        'Duration_Redbus': 'Durasi Redbus (Min)', 'Duration_Traveloka': 'Durasi Traveloka (Min)',
                        'Departing_Time': 'Waktu Berangkat', 'Route_Date': 'Tanggal Trip'
                    }
                ), 
                use_container_width=True
            )
        else:
            st.warning("Tidak ada data yang tersedia untuk tanggal yang dipilih.")

        # -------------------------------------------------------------
        # 4. KOMPARASI HARGA BERDASARKAN RUTE, PO, DAN TIPE
        # -------------------------------------------------------------
        st.header("4. Komparasi Harga Berdasarkan Rute, PO Bus, dan Tipe Bus")
        st.markdown("Tabel ini menunjukkan perbandingan **Harga Rata-rata** untuk setiap layanan bus (`PO Bus` dan `Tipe Bus`) pada masing-masing rute.")

        route_service_comparison_df = analyze_service_price_by_route(df_redbus_clean, df_traveloka_clean)
        
        if route_service_comparison_df.empty:
            st.warning("Tidak ada data rute/layanan yang cukup valid untuk perbandingan.")
        else:
            all_routes_svc = route_service_comparison_df['Rute'].unique()
            selected_routes_svc = st.multiselect(
                "Filter Rute:",
                options=all_routes_svc,
                default=all_routes_svc,
                key="route_svc_filter"
            )

            filtered_route_svc_df = route_service_comparison_df[route_service_comparison_df['Rute'].isin(selected_routes_svc)]
            
            # Tampilkan tabel utama
            st.dataframe(filtered_route_svc_df, use_container_width=True)
            
            st.markdown("---")
            st.subheader("Ringkasan Komparasi PO Bus pada Rute yang Dipilih")

            # --- Ringkasan 1: POs yang TIDAK Melayani Rute yang Dipilih ---
            po_in_filtered_routes = filtered_route_svc_df['PO Bus'].unique()
            po_not_serving = set(df_all_po) - set(po_in_filtered_routes)
            
            with st.expander("PO Bus yang Tidak Menyediakan Layanan di Rute yang Dipilih"):
                if po_not_serving:
                    st.markdown(f"**Total PO tidak melayani rute yang dipilih:** **{len(po_not_serving)}**")
                    st.code(", ".join(sorted(list(po_not_serving))))
                else:
                    st.info("Semua PO yang ada dalam data menyediakan layanan pada rute yang dipilih.")

            # --- Ringkasan 2 & 3: Harga Termurah dan Termahal per PO ---
            df_filtered_raw = df_combined[
                (df_combined['Route_Name'].isin(selected_routes_svc)) & 
                (df_combined['Price'] > 0)
            ].copy()
            
            if not df_filtered_raw.empty:
                
                # --- Cari Harga Termurah per PO ---
                idx_min = df_filtered_raw.groupby('Bus_Name')['Price'].idxmin()
                cheapest_services = df_filtered_raw.loc[idx_min, ['Bus_Name', 'Bus_Type', 'Price', 'OTA', 'Route_Name']].reset_index(drop=True)
                cheapest_services = cheapest_services.rename(columns={
                    'Bus_Name': 'PO Bus',
                    'Price': 'Harga Termurah', 
                    'Bus_Type': 'Tipe Bus Termurah', 
                    'OTA': 'Platform',
                    'Route_Name': 'Rute Terkait'
                })
                cheapest_services['Harga Termurah'] = cheapest_services['Harga Termurah'].astype(int)
                
                # --- Cari Harga Termahal per PO ---
                idx_max = df_filtered_raw.groupby('Bus_Name')['Price'].idxmax()
                expensive_services = df_filtered_raw.loc[idx_max, ['Bus_Name', 'Bus_Type', 'Price', 'OTA', 'Route_Name']].reset_index(drop=True)
                expensive_services = expensive_services.rename(columns={
                    'Bus_Name': 'PO Bus',
                    'Price': 'Harga Termahal', 
                    'Bus_Type': 'Tipe Bus Termahal', 
                    'OTA': 'Platform',
                    'Route_Name': 'Rute Terkait'
                })
                expensive_services['Harga Termahal'] = expensive_services['Harga Termahal'].astype(int)

                col_min, col_max = st.columns(2)
                
                with col_min:
                    st.markdown("#### 🏆 Layanan Termurah per PO Bus")
                    st.markdown("*Harga terendah yang pernah ditemukan untuk PO ini di rute yang dipilih, terlepas dari Tipe Bus.*")
                    st.dataframe(
                        cheapest_services.sort_values(by='Harga Termurah', ascending=True),
                        use_container_width=True
                    )
                    
                with col_max:
                    st.markdown("#### 💵 Layanan Termahal per PO Bus")
                    st.markdown("*Harga tertinggi yang pernah ditemukan untuk PO ini di rute yang dipilih, terlepas dari Tipe Bus.*")
                    st.dataframe(
                        expensive_services.sort_values(by='Harga Termahal', ascending=False),
                        use_container_width=True
                    )

            else:
                st.info("Data mentah tidak memiliki harga valid untuk PO bus di rute yang dipilih.")


        # -------------------------------------------------------------
        # 5. PERBANDINGAN HARGA RATA-RATA OTA (Outer Join - Global)
        # -------------------------------------------------------------
        st.header("5. Komparasi Harga Rata-rata (Redbus vs. Traveloka) - Global")
        st.markdown("*Analisis ini membandingkan harga rata-rata antar PO bus tanpa memperhatikan rute.*")
        
        comparison_df = perform_comparison(df_redbus_clean, df_traveloka_clean)
        
        st.subheader("Tabel Perbandingan Harga Rata-rata Semua Layanan")
        st.dataframe(comparison_df.rename(columns={'Harga Redbus': 'Harga Redbus (Rata-rata Global)', 'Harga Traveloka': 'Harga Traveloka (Rata-rata Global)'}), use_container_width=True)
        
        # -------------------------------------------------------------
        # 6. Ringkasan dan Visualisasi
        # -------------------------------------------------------------
        st.header("6. Ringkasan Layanan")
        
        col1, col2 = st.columns(2)
        
        cheaper_counts = comparison_df['Status Harga'].value_counts()
        
        with col1:
            st.metric(label="Total Layanan Bus Unik Ditemukan", value=len(comparison_df))
            st.metric(label="Hanya di Redbus", value=cheaper_counts.get('Hanya Redbus', 0))
            st.metric(label="Hanya di Traveloka", value=cheaper_counts.get('Hanya Traveloka', 0))
            st.metric(label="Layanan dengan Harga Sama", value=cheaper_counts.get('Harga Sama / Tidak Ada', 0))

        with col2:
            st.markdown("#### Distribusi Status Harga")
            fig, ax = plt.subplots(figsize=(8, 5))
            sns.barplot(x=cheaper_counts.index, y=cheaper_counts.values, ax=ax, palette='viridis')
            ax.set_title('Status Ketersediaan dan Keunggulan Harga')
            ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha='right')
            st.pyplot(fig)