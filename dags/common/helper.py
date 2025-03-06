import time

from common.hook import rs_hook


def call_procedure(proc_name: str):
    connection = rs_hook.get_conn()
    cursor = connection.cursor()
    start_time = time.time()
    print("Connecting to redshift")
    query = f"""call {proc_name}"""
    cursor.execute(query)
    end_time = time.time()
    print(
        f"Query execution time takes about {end_time - start_time} seconds")
    cursor.commit()
    cursor.close()
