import psycopg2
from psycopg2 import sql


def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS clients (
            client_id SERIAL PRIMARY KEY,
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            email VARCHAR(100) UNIQUE
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS phones (
            phone_id SERIAL PRIMARY KEY,
            client_id INTEGER REFERENCES clients(client_id),
            phone_number VARCHAR(20) UNIQUE
        );
        """)
    conn.commit()




def add_client(conn, first_name, last_name, email, phones=None):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO clients (first_name, last_name, email) VALUES (%s, %s, %s) RETURNING client_id;
        """, (first_name, last_name, email))
        client_id = cur.fetchone()[0]
        if phones:
            for phone in phones:
                cur.execute("""
                INSERT INTO phones (client_id, phone_number) VALUES (%s, %s);
                """, (client_id, phone))
    conn.commit()


def add_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO phones (client_id, phone_number) VALUES (%s, %s);
        """, (client_id, phone))
    conn.commit()


def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    with conn.cursor() as cur:
        if first_name or last_name or email:
            columns = []
            values = []
            if first_name:
                columns.append('first_name = %s')
                values.append(first_name)
            if last_name:
                columns.append('last_name = %s')
                values.append(last_name)
            if email:
                columns.append('email = %s')
                values.append(email)
            values.append(client_id)
            cur.execute(sql.SQL("UPDATE clients SET {} WHERE client_id = %s;").format(
                sql.SQL(", ").join(map(sql.SQL, columns))), values)

        if phones:
            for phone in phones:
                add_phone(conn, client_id, phone)
    conn.commit()


def delete_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM phones WHERE client_id=%s AND phone_number=%s;
        """, (client_id, phone))
    conn.commit()


def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM phones WHERE client_id=%s;
        """, (client_id,))
        cur.execute("""
        DELETE FROM clients WHERE client_id=%s;
        """, (client_id,))
    conn.commit()



def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    with conn.cursor() as cur:
        if phone:
            cur.execute("""
            SELECT c.client_id, first_name, last_name, email, phone_number FROM clients c
            INNER JOIN phones p ON c.client_id = p.client_id WHERE p.phone_number=%s
            """, (phone,))
            return cur.fetchall()
        else:
            conditions_and_values = []
            if first_name:
                conditions_and_values.append(("first_name=%s", first_name))
            if last_name:
                conditions_and_values.append(("last_name=%s", last_name))
            if email:
                conditions_and_values.append(("email=%s", email))

            condition_strs, values = zip(*conditions_and_values) if conditions_and_values else ((), ())
            sql_query = "SELECT client_id, first_name, last_name, email FROM clients"
            if condition_strs:
                sql_query += " WHERE " + " AND ".join(condition_strs)

                cur.execute(sql.SQL(sql_query), values)
                return cur.fetchall()

    with psycopg2.connect(database="clients_db", user="postgres", password="postgres") as conn:
        create_db(conn)
        add_client(conn, "Игорь", "Иванов", "johndoe@example.com", ["+123456789"])
        print(find_client(conn, first_name="Иван"))
        add_phone(conn, 1, "+987619376")
        change_client(conn, 1, first_name="Кирилл")
        print(find_client(conn, first_name="Александр"))
        delete_phone(conn, 1, "+987294751")
        delete_client(conn, 1)


    conn.close()



