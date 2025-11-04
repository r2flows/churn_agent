#!/usr/bin/env python3
"""
Script para descargar archivos CSV del bucket S3 etl-vendors-bi/Vendor_oportunities/
Solo descarga archivos .csv a la carpeta data/
"""

import boto3
import os
from pathlib import Path
from botocore.exceptions import NoCredentialsError, ClientError

def download_csv_files_from_s3():
    """
    Descarga todos los archivos CSV del bucket S3 especificado
    """
    # Configuración
    bucket_name = 'etl-vendors-bi'
    s3_prefix = 'Vendor_oportunities/'
    local_folder = 'data/'
    
    # Crear carpeta local si no existe
    Path(local_folder).mkdir(exist_ok=True)
    
    try:
        # Inicializar cliente S3
        s3_client = boto3.client('s3')
        
        print(f"🔍 Buscando archivos CSV en s3://{bucket_name}/{s3_prefix}")
        
        # Listar objetos en el bucket con el prefijo especificado
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=s3_prefix
        )
        
        # Verificar si hay objetos
        if 'Contents' not in response:
            print(f"❌ No se encontraron archivos en s3://{bucket_name}/{s3_prefix}")
            return
        
        csv_files = []
        
        # Filtrar solo archivos CSV (no carpetas)
        for obj in response['Contents']:
            key = obj['Key']
            
            # Verificar que sea un archivo CSV y no una carpeta
            if key.lower().endswith('.csv') and not key.endswith('/'):
                csv_files.append(key)
        
        if not csv_files:
            print(f"❌ No se encontraron archivos CSV en s3://{bucket_name}/{s3_prefix}")
            return
        
        print(f"📂 Encontrados {len(csv_files)} archivos CSV:")
        for file in csv_files:
            print(f"   - {file}")
        
        print(f"\n⬇️  Iniciando descarga a la carpeta '{local_folder}'...")
        
        # Descargar cada archivo CSV
        downloaded_files = []
        failed_files = []
        
        for s3_key in csv_files:
            try:
                # Obtener solo el nombre del archivo (sin la ruta S3)
                filename = os.path.basename(s3_key)
                local_path = os.path.join(local_folder, filename)
                
                print(f"📥 Descargando: {filename}")
                
                # Descargar archivo
                s3_client.download_file(bucket_name, s3_key, local_path)
                
                # Verificar que el archivo se descargó correctamente
                if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
                    downloaded_files.append(filename)
                    print(f"✅ {filename} descargado exitosamente")
                else:
                    failed_files.append(filename)
                    print(f"❌ Error al descargar {filename} (archivo vacío o no existe)")
                
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == '404':
                    print(f"❌ Archivo no encontrado: {s3_key}")
                else:
                    print(f"❌ Error al descargar {s3_key}: {e}")
                failed_files.append(os.path.basename(s3_key))
            
            except Exception as e:
                print(f"❌ Error inesperado al descargar {s3_key}: {e}")
                failed_files.append(os.path.basename(s3_key))
        
        # Resumen final
        print(f"\n📊 RESUMEN DE DESCARGA:")
        print(f"✅ Archivos descargados exitosamente: {len(downloaded_files)}")
        if downloaded_files:
            for file in downloaded_files:
                print(f"   - {file}")
        
        if failed_files:
            print(f"❌ Archivos que fallaron: {len(failed_files)}")
            for file in failed_files:
                print(f"   - {file}")
        
        print(f"\n📁 Archivos guardados en: {os.path.abspath(local_folder)}")
        
    except NoCredentialsError:
        print("❌ Error: No se encontraron credenciales de AWS")
        print("💡 Configura tus credenciales usando:")
        print("   - aws configure")
        print("   - Variables de entorno AWS_ACCESS_KEY_ID y AWS_SECRET_ACCESS_KEY")
        print("   - Perfil IAM si estás en EC2")
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchBucket':
            print(f"❌ Error: El bucket '{bucket_name}' no existe")
        elif error_code == 'AccessDenied':
            print(f"❌ Error: Sin permisos para acceder al bucket '{bucket_name}'")
        else:
            print(f"❌ Error de cliente AWS: {e}")
    
    except Exception as e:
        print(f"❌ Error inesperado: {e}")

def list_csv_files_only():
    """
    Solo lista los archivos CSV disponibles sin descargarlos
    """
    bucket_name = 'etl-vendors-bi'
    s3_prefix = 'Vendor_oportunities/'
    
    try:
        s3_client = boto3.client('s3')
        
        print(f"🔍 Listando archivos CSV en s3://{bucket_name}/{s3_prefix}")
        
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=s3_prefix
        )
        
        if 'Contents' not in response:
            print(f"❌ No se encontraron archivos en s3://{bucket_name}/{s3_prefix}")
            return
        
        csv_files = []
        for obj in response['Contents']:
            key = obj['Key']
            if key.lower().endswith('.csv') and not key.endswith('/'):
                csv_files.append({
                    'name': os.path.basename(key),
                    'full_path': key,
                    'size_mb': round(obj['Size'] / 1024 / 1024, 2),
                    'last_modified': obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
                })
        
        if not csv_files:
            print(f"❌ No se encontraron archivos CSV")
            return
        
        print(f"\n📂 Archivos CSV encontrados ({len(csv_files)}):")
        print(f"{'Archivo':<40} {'Tamaño (MB)':<12} {'Última modificación'}")
        print("-" * 70)
        
        for file in csv_files:
            print(f"{file['name']:<40} {file['size_mb']:<12} {file['last_modified']}")
        
    except Exception as e:
        print(f"❌ Error al listar archivos: {e}")

if __name__ == "__main__":
    import sys
    
    print("🚀 Script de descarga de archivos CSV desde S3")
    print("=" * 50)
    
    if len(sys.argv) > 1 and sys.argv[1] == "--list":
        list_csv_files_only()
    else:
        download_csv_files_from_s3()
        
        print("\n💡 Tip: Usa '--list' para solo ver los archivos disponibles:")
        print("   python download_s3_csvs.py --list")