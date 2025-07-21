def get_existing_keys(cursor, table_name: str, key_column: str):
    try:
        query = f"SELECT {key_column} FROM {table_name} WHERE is_current = TRUE"
        print(f"Đang chạy query: {query}")  # In query để debug
        cursor.execute(query)
        rows = cursor.fetchall()
        print(f"Số dòng lấy được: {len(rows)}")  # <- In số lượng
        return [row[0] for row in rows]
    except Exception as e:
        print(f"Lỗi khi lấy khoá từ bảng {table_name}: {e}")
        return []
