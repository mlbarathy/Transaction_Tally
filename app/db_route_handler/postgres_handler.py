import psycopg2
from app.utils.config import POSTGRESS_CON

class PostgresHandler:
    def __init__(self):
        self.conn = psycopg2.connect(**POSTGRESS_CON)

    def insert_or_update(self, data, main_table, history_table, pk):
        cursor = self.conn.cursor()
        
        # Handle both single and composite primary keys
        pk_list = pk if isinstance(pk, list) else [pk]
        quoted_main_table = f'"{main_table}"'
        quoted_history_table = f'"{history_table}"'

        for record in data:
            # Build WHERE clause for composite keys
            where_conditions = []
            where_values = []
            
            for pk_col in pk_list:
                quoted_pk_col = f'"{pk_col}"'
                where_conditions.append(f"{quoted_pk_col} = %s")
                where_values.append(record[pk_col])
            
            where_clause = " AND ".join(where_conditions)
            
            cursor.execute(
                f"SELECT * FROM {quoted_main_table} WHERE {where_clause}",
                tuple(where_values)
            )
            existing = cursor.fetchone()
            if existing:
                columns = [desc[0] for desc in cursor.description]
                existing_dict = dict(zip(columns, existing))
                cursor.execute(f"SELECT * FROM {quoted_main_table} WHERE {where_clause}", tuple(where_values))
                existing_rows = cursor.fetchall()

                if existing_rows:
                    columns = [desc[0] for desc in cursor.description]

                    cursor.execute(
                        f"SELECT MAX(version) FROM {quoted_history_table} WHERE {where_clause}",
                        tuple(where_values)
                    )
                    max_ver_result = cursor.fetchone()[0]
                    version_to_insert = int(max_ver_result) + 1 if max_ver_result is not None else 1
                    quoted_columns = ', '.join(f'"{col}"' for col in columns)
                    history_cols = f"{quoted_columns}, version"
                    placeholders = ', '.join(['%s'] * len(columns)) + ', %s'

                    for existing in existing_rows:
                        existing_dict = dict(zip(columns, existing))
                        history_vals = list(existing_dict.values()) + [version_to_insert]
                        cursor.execute(
                            f"INSERT INTO {quoted_history_table} ({history_cols}) VALUES ({placeholders})",
                            history_vals
                        )

                    update_cols = ', '.join([f'"{key}" = %s' for key in record.keys()])
                    update_vals = list(record.values()) + where_values
                    cursor.execute(
                        f"UPDATE {quoted_main_table} SET {update_cols} WHERE {where_clause}",
                        update_vals
                    )
            else:
                insert_cols = ', '.join(f'"{col}"' for col in record.keys())
                insert_placeholders = ', '.join(['%s'] * len(record))
                cursor.execute(
                    f"INSERT INTO {quoted_main_table} ({insert_cols}) VALUES ({insert_placeholders})",
                    tuple(record.values())
                )
        self.conn.commit()
        cursor.close()
