from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.exceptions import AirflowException
 
 
def consulta_oracle():
    # Conexiones a Oracle
    oracle_source = OracleHook(oracle_conn_id="oracle_source")
    oracle_target = OracleHook(oracle_conn_id="oracle_target")
 
    # Nombre de la tabla origen y destino
    source_table = "TABLA_EJEMPLO_1"
    target_table = "TABLA_JOSELIN"
 
    # Obtener los datos de la tabla origen
    source_sql = f"SELECT * FROM {source_table}"
    result = oracle_source.get_records(source_sql)
 
    if not result:
        print("No hay datos para insertar.")
        return
 
    # Obtener la estructura de la tabla origen
    columns_sql = """
        SELECT column_name, data_type, nullable, data_length, data_precision, data_scale
        FROM all_tab_columns
        WHERE table_name = :table_name
        AND owner = 'SOURCE'
        ORDER BY column_id
    """
    columns = oracle_source.get_records(
        columns_sql, parameters={"table_name": source_table.upper()}
    )
 
    if not columns:
        raise AirflowException(
            f"No se encontraron columnas para la tabla {source_table}."
        )
 
    # Verificar si la tabla de destino existe
    check_table_sql = f"""
        SELECT table_name
        FROM user_tables
        WHERE table_name = UPPER(:table_name)
    """
    table_exists = oracle_target.get_first(
        check_table_sql, parameters={"table_name": target_table.upper()}
    )
 
    if not table_exists:
        print(f"La tabla {target_table} no existe. Creándola ahora.")
        # Generar la sentencia CREATE TABLE dinámicamente
        create_table_sql = f"CREATE TABLE {target_table} (\n"
        column_definitions = []
 
        for column in columns:
            (
                column_name,
                data_type,
                nullable,
                data_length,
                data_precision,
                data_scale,
            ) = column
            column_def = f"    {column_name} {map_data_type(data_type, data_length, data_precision, data_scale)}"
            if nullable == "N":
                column_def += " NOT NULL"
            column_definitions.append(column_def)
 
        create_table_sql += ",\n".join(column_definitions)
        create_table_sql += "\n)"
 
        try:
            oracle_target.run(create_table_sql)
            print(f"Tabla {target_table} creada exitosamente.")
        except Exception as e:
            raise AirflowException(f"Error al crear la tabla {target_table}: {e}")
    else:
        print(f"La tabla {target_table} ya existe.")
 
    # Preparar la sentencia de inserción
    column_names = [col[0] for col in columns]
    placeholders = ", ".join([f":{i+1}" for i in range(len(column_names))])
    insert_sql = f"""
        INSERT INTO {target_table} ({', '.join(column_names)})
        VALUES ({placeholders})
    """
 
    try:
        # Insertar cada fila en la tabla de destino
        for row in result:
            oracle_target.run(insert_sql, parameters=row)
        print(f"{len(result)} filas insertadas en {target_table} exitosamente.")
    except Exception as e:
        raise AirflowException(f"Error al insertar datos en {target_table}: {e}")
 
 
def map_data_type(data_type, data_length, data_precision, data_scale):
    """
    Mapea los tipos de datos de Oracle a una representación adecuada para la sentencia CREATE TABLE.
    Ajusta según tus necesidades específicas.
    """
    data_type = data_type.upper()
    if data_type in ["VARCHAR2", "CHAR"]:
        return f"{data_type}({data_length})"
    elif data_type == "NUMBER":
        if data_precision and data_scale:
            return f"NUMBER({data_precision}, {data_scale})"
        elif data_precision:
            return f"NUMBER({data_precision})"
        else:
            return "NUMBER"
    elif data_type == "DATE":
        return "DATE"
    elif data_type == "TIMESTAMP":
        return "TIMESTAMP"
    elif data_type == "CLOB":
        return "CLOB"
    elif data_type == "BLOB":
        return "BLOB"
    # Agrega más tipos de datos según sea necesario
    else:
        raise AirflowException(f"Tipo de dato no soportado: {data_type}")
 
 
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 12, 10),
}
 
dag = DAG(
    "oracle_connection_example_dynamic",
    default_args=default_args,
    schedule_interval=None,
)
 
t1 = PythonOperator(
    task_id="consulta_oracle",
    python_callable=consulta_oracle,
    dag=dag,
)