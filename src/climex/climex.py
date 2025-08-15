"""
NASA POWER API Function for DataFrame input
Función simplificada para descargar datos climáticos usando pandas DataFrame
"""
import os, sys, time, json, urllib3, requests, multiprocessing
import pandas as pd
from typing import List

urllib3.disable_warnings()

def download_function(collection):
    """Función de descarga para multiprocesamiento - guarda directamente en CSV"""
    request, filepath, row_info = collection
    try:
        response = requests.get(url=request, verify=False, timeout=30.00).json()
        
        # Convertir respuesta JSON a CSV
        if 'properties' in response and 'parameter' in response['properties']:
            climate_params = response['properties']['parameter']
            
            # Convertir a DataFrame
            df_climate = pd.DataFrame(climate_params)
            
            if not df_climate.empty:
                # Agregar metadata
                df_climate['latitude'] = row_info['latitude']
                df_climate['longitude'] = row_info['longitude']
                df_climate['identifier'] = row_info['identifier']
                
                # Manejar fechas/períodos
                if df_climate.index.name is None and len(df_climate.index) > 0:
                    try:
                        df_climate['date'] = pd.to_datetime(df_climate.index, format='%Y%m%d')
                    except:
                        df_climate['period'] = df_climate.index
                
                # Reordenar columnas
                base_cols = ['identifier', 'latitude', 'longitude']
                if 'date' in df_climate.columns:
                    base_cols.append('date')
                if 'period' in df_climate.columns:
                    base_cols.append('period')
                    
                other_cols = [col for col in df_climate.columns if col not in base_cols]
                df_climate = df_climate[base_cols + other_cols]
                
                # Guardar como CSV
                df_climate.to_csv(filepath, index=False)
                
        return row_info, True, None
        
    except Exception as e:
        return row_info, False, str(e)

def download_nasa_power_data(df: pd.DataFrame,
                           lat_col: str = 'latitude',
                           lon_col: str = 'longitude',
                           start_date: str = '20150101',
                           end_date: str = '20150305',
                           parameters: List[str] = ['T2M', 'T2MDEW', 'T2MWET', 'TS', 'T2M_RANGE', 'T2M_MAX', 'T2M_MIN'],
                           temporal_resolution: str = 'daily',
                           spatial_resolution: str = 'point',
                           community: str = 'RE',
                           processes: int = 5,
                           output_folder: str = './nasa_power_data',
                           start_col: str = None,
                           end_col: str = None,
                           id_col: str = None,
                           return_consolidated: bool = True) -> pd.DataFrame:
    """
    Descarga datos climáticos de NASA POWER para ubicaciones en un DataFrame
    
    Args:
        df: DataFrame con datos de ubicaciones
        lat_col: Nombre de la columna de latitud (default: 'latitude')
        lon_col: Nombre de la columna de longitud (default: 'longitude')
        start_date: Fecha de inicio en formato YYYYMMDD (default: '20150101')
        end_date: Fecha de fin en formato YYYYMMDD (default: '20150305')
        parameters: Lista de parámetros climáticos a descargar (default: temperatura y derivados)
        resolution: Resolución temporal ('daily', 'monthly', 'climatology') (default: 'daily')
        community: Comunidad NASA POWER ('RE', 'AG', 'SB') (default: 'RE')
        processes: Número de procesos concurrentes (máximo 5) (default: 5)
        start_col: Nombre de columna con fechas de inicio (opcional)
        end_col: Nombre de columna con fechas de fin (opcional)
        id_col: Nombre de columna para identificador único (opcional)
        output_folder: Carpeta donde guardar archivos JSON (default: './nasa_power_data')
        return_consolidated: Si retornar datos consolidados o solo resultados de descarga (default: True)
        
    Returns:
        DataFrame con datos climáticos consolidados (si return_consolidated=True) 
        o DataFrame con resultados de descarga (si return_consolidated=False)
        
    Parámetros disponibles:
        Temperatura: T2M, T2M_MAX, T2M_MIN, T2M_RANGE, T2MDEW, T2MWET, TS
        Radiación: ALLSKY_SFC_SW_DWN, CLRSKY_SFC_SW_DWN, ALLSKY_SFC_LW_DWN
        Precipitación: PRECTOTCORR
        Viento: WS2M, WS10M, WD2M, WD10M
        Humedad: RH2M, QV2M
        Presión: PS, SLP
        
    Resoluciones temporales disponibles:
        - 'daily': Datos diarios
        - 'monthly': Promedios mensuales
        - 'climatology': Climatología (promedios de largo plazo)
    
    Resoluciones espaciales disponibles:
        - 'point': Datos puntuales (0.5° x 0.625°)
        - 'regional': Datos regionales (1° x 1°)
        
    Comunidades disponibles:
        - 'RE': Renewable Energy (Energía Renovable)
        - 'AG': Agroclimatology (Agroclimatología)
        - 'SB': Sustainable Buildings (Edificios Sustentables)
    """
    
    # Validaciones
    if lat_col not in df.columns or lon_col not in df.columns:
        raise ValueError(f"DataFrame debe contener columnas '{lat_col}' y '{lon_col}'")
    
    if not parameters:
        raise ValueError("Debe especificar al menos un parámetro")
    
    processes = min(processes, 5)  # NASA recomienda máximo 5 requests concurrentes
    
    valid_temporal_resolutions = ['daily', 'monthly', 'climatology']
    if temporal_resolution not in valid_temporal_resolutions:
        raise ValueError(f"Resolución temporal debe ser una de: {valid_temporal_resolutions}")
    
    valid_spatial_resolutions = ['point', 'regional']
    if spatial_resolution not in valid_spatial_resolutions:
        raise ValueError(f"Resolución espacial debe ser una de: {valid_spatial_resolutions}")
    
    valid_communities = ['RE', 'AG', 'SB']
    if community not in valid_communities:
        raise ValueError(f"Comunidad debe ser una de: {valid_communities}")
    
    # Crear directorio de salida
    os.makedirs(output_folder, exist_ok=True)
    
    # Template de URL con parámetros dinámicos
    params_str = ','.join(parameters)
    
    if temporal_resolution == 'climatology':
        request_template = (
            "https://power.larc.nasa.gov/api/temporal/climatology/{spatial_resolution}?"
            "parameters={parameters}&community={community}&"
            "longitude={{longitude}}&latitude={{latitude}}&format=JSON"
        ).format(spatial_resolution=spatial_resolution, parameters=params_str, community=community)
    else:
        request_template = (
            "https://power.larc.nasa.gov/api/temporal/{temporal_resolution}/{spatial_resolution}?"
            "parameters={parameters}&community={community}&"
            "longitude={{longitude}}&latitude={{latitude}}&"
            "start={{start_date}}&end={{end_date}}&format=JSON"
        ).format(temporal_resolution=temporal_resolution, spatial_resolution=spatial_resolution, 
                parameters=params_str, community=community)
    
    start_time = time.time()
    requests = []
    
    print(f"Preparando {len(df)} solicitudes de descarga...")
    print(f"Parámetros: {', '.join(parameters)}")
    print(f"Resolución temporal: {temporal_resolution}")
    print(f"Resolución espacial: {spatial_resolution}")
    print(f"Comunidad: {community}")
    print(f"Procesos: {processes}")
    
    # Preparar requests para cada fila del DataFrame
    for idx, row in df.iterrows():
        latitude = row[lat_col]
        longitude = row[lon_col]
        
        # Manejar columnas de fecha
        if start_col and start_col in df.columns:
            start = pd.to_datetime(row[start_col]).strftime('%Y%m%d')
        else:
            start = start_date
            
        if end_col and end_col in df.columns:
            end = pd.to_datetime(row[end_col]).strftime('%Y%m%d')
        else:
            end = end_date
        
        # Crear URL de request
        if temporal_resolution == 'climatology':
            request_url = request_template.format(
                latitude=latitude, 
                longitude=longitude
            )
            date_suffix = "climatology"
        else:
            request_url = request_template.format(
                latitude=latitude, 
                longitude=longitude,
                start_date=start,
                end_date=end
            )
            date_suffix = f"{start}_{end}"
        
        # Crear nombre de archivo CSV
        identifier = row[id_col] if id_col and id_col in df.columns else f"idx_{idx}"
        #filename = f"{identifier}_Lat_{latitude}_Lon_{longitude}_{date_suffix}.csv"
        filename = f"{identifier}.csv"
        filepath = os.path.join(output_folder, filename)
        
        # Información de la fila para tracking
        row_info = {
            'index': idx,
            'latitude': latitude,
            'longitude': longitude,
            'start_date': start if temporal_resolution != 'climatology' else None,
            'end_date': end if temporal_resolution != 'climatology' else None,
            'identifier': identifier,
            'filename': filename
        }
        
        requests.append((request_url, filepath, row_info))
    
    # Ejecutar descargas con multiprocesamiento
    print(f"\nIniciando descargas...")
    requests_total = len(requests)
    
    with multiprocessing.Pool(processes) as pool:
        results = pool.imap_unordered(download_function, requests)
        
        download_results = []
        for i, (row_info, success, error) in enumerate(results, 1):
            sys.stderr.write(f'\rDescargando: {i/requests_total:.1%}')
            
            result = row_info.copy()
            result['success'] = success
            result['error'] = error
            result['downloaded_at'] = time.strftime('%Y-%m-%d %H:%M:%S')
            download_results.append(result)
    
    # Crear DataFrame de resultados
    results_df = pd.DataFrame(download_results)
    
    total_time = round((time.time() - start_time), 2)
    
    print(f"\n\n¡Descarga completada!")
    print(f"Tiempo total: {total_time} segundos")
    print(f"Descargas exitosas: {results_df['success'].sum()}/{len(results_df)}")
    
    if not results_df['success'].all():
        failed = results_df[~results_df['success']]
        print(f"Descargas fallidas: {len(failed)}")
    
    # Si se solicita, consolidar datos
    if return_consolidated and results_df['success'].any():
        print("\nConsolidando datos...")
        consolidated_data = _consolidate_csv_data(results_df, output_folder)
        return consolidated_data
    else:
        return results_df

def _consolidate_csv_data(results_df: pd.DataFrame, output_folder: str) -> pd.DataFrame:
    """Función auxiliar para consolidar archivos CSV en un DataFrame"""
    all_data = []
    successful_downloads = results_df[results_df['success']]
    
    for _, row in successful_downloads.iterrows():
        filepath = os.path.join(output_folder, row['filename'])
        
        try:
            # Leer archivo CSV
            df_climate = pd.read_csv(filepath)
            
            if not df_climate.empty:
                all_data.append(df_climate)
                    
        except Exception as e:
            print(f"Error cargando {row['filename']}: {e}")
    
    if all_data:
        consolidated_df = pd.concat(all_data, ignore_index=True)
        print(f"Datos consolidados: {len(consolidated_df)} registros")
        return consolidated_df
    else:
        print("No se pudieron consolidar datos")
        return pd.DataFrame()
