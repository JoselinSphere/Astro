from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
 
 
def map_data_type(
    data_type, character_maximum_length, numeric_precision, numeric_scale
):
    """
    Mapea los tipos de datos de Snowflake a una representación adecuada para la sentencia CREATE TABLE.
    Ajusta según tus necesidades específicas.
    """
    data_type = data_type.upper()
    if data_type in ["VARCHAR", "CHAR", "STRING", "TEXT"]:
        if character_maximum_length:
            return f"VARCHAR({character_maximum_length})"
        else:
            return "VARCHAR"
    elif data_type in ["NUMBER", "NUMERIC", "DECIMAL"]:
        if numeric_precision and numeric_scale:
            return f"NUMBER({numeric_precision}, {numeric_scale})"
        elif numeric_precision:
            return f"NUMBER({numeric_precision})"
        else:
            return "NUMBER"
    elif data_type == "DATE":
        return "DATE"
    elif data_type in ["TIMESTAMP_LTZ", "TIMESTAMP_NTZ", "TIMESTAMP_TZ"]:
        return data_type
    elif data_type == "BOOLEAN":
        return "BOOLEAN"
    elif data_type == "BINARY":
        return "BINARY"
    # Agrega más tipos de datos según sea necesario
    else:
        raise AirflowException(f"Tipo de dato no soportado: {data_type}")
 
 
def consulta_snowflake():
    # Obtener detalles de la conexión de origen
    conn_source = BaseHook.get_connection("snowflake_source")
    database_source = conn_source.extra_dejson.get("database")
    schema_source = "SOURCE"
 
    # Obtener detalles de la conexión de destino
    conn_target = BaseHook.get_connection("snowflake_target")
    database_target = conn_target.extra_dejson.get("database")
    schema_target = "TARGET"
 
    # Inicializa los hooks de Snowflake para fuente y destino
    snowflake_source = SnowflakeHook(snowflake_conn_id="snowflake_source")
    snowflake_target = SnowflakeHook(snowflake_conn_id="snowflake_target")
 
    # Nombre de la tabla origen y destino
    source_table = "TABLA_PRUEBA"
    target_table = "TABLA_PRUEBA"
 
    # Obtener los datos de la tabla origen
    source_sql = f"SELECT * FROM {database_source}.{schema_source}.{source_table}"
    result = snowflake_source.get_records(source_sql)
 
    if not result:
        print("No hay datos para insertar.")
        return
 
    # Obtener la estructura de la tabla origen
    columns_sql = f"""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
        FROM {database_source}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{source_table.upper()}'
        AND TABLE_SCHEMA = '{schema_source.upper()}'
        ORDER BY ORDINAL_POSITION
    """
    columns = snowflake_source.get_records(columns_sql)
 
    if not columns:
        raise AirflowException(
            f"No se encontraron columnas para la tabla {source_table}."
        )
 
    # Verificar si la tabla de destino existe
    check_table_sql = f"""
        SELECT TABLE_NAME
        FROM {database_target}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME = '{target_table.upper()}'
        AND TABLE_SCHEMA = '{schema_target.upper()}'
    """
    table_exists = snowflake_target.get_first(check_table_sql)
 
    if not table_exists:
        print(f"La tabla {target_table} no existe. Creándola ahora.")
        # Generar la sentencia CREATE TABLE dinámicamente
        create_table_sql = (
            f"CREATE TABLE {database_target}.{schema_target}.{target_table} (\n"
        )
        column_definitions = []
 
        for column in columns:
            (
                column_name,
                data_type,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
            ) = column
            column_def = f"    {column_name} {map_data_type(data_type, character_maximum_length, numeric_precision, numeric_scale)}"
            # En Snowflake, puedes definir si una columna es NULL o NOT NULL si lo deseas
            # Por simplicidad, asumiremos que todas las columnas permiten NULL
            # Si deseas manejar esto, necesitarás obtener esa información adicionalmente
            column_definitions.append(column_def)
 
        create_table_sql += ",\n".join(column_definitions)
        create_table_sql += "\n)"
 
        try:
            snowflake_target.run(create_table_sql)
            print(f"Tabla {target_table} creada exitosamente.")
        except Exception as e:
            raise AirflowException(f"Error al crear la tabla {target_table}: {e}")
    else:
        print(f"La tabla {target_table} ya existe.")
 
    # Preparar la sentencia de inserción
    column_names = [col[0] for col in columns]
    placeholders = ", ".join(["%s" for _ in column_names])
    insert_sql = f"""
        INSERT INTO {database_target}.{schema_target}.{target_table} ({', '.join(column_names)})
        VALUES ({placeholders})
    """
 
    try:
        # Insertar todas las filas utilizando executemany para optimizar el rendimiento
        conn = snowflake_target.get_conn()
        cursor = conn.cursor()
        cursor.executemany(insert_sql, result)
        conn.commit()
        cursor.close()
        print(f"{len(result)} filas insertadas en {target_table} exitosamente.")
    except Exception as e:
        raise AirflowException(f"Error al insertar datos en {target_table}: {e}")
 
 
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 12, 10),
}
 
dag = DAG(
    "snowflake_connection_copy",
    default_args=default_args,
    schedule_interval=None,
)
 
t1 = PythonOperator(
    task_id="consulta_snowflake",
    python_callable=consulta_snowflake,
    dag=dag,
)