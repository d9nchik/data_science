from json import load
import psycopg2

conn = psycopg2.connect(dbname='data_science', user='d9nich',
                        password="J'~3)Yq=JpFAzQMT", host='localhost')
cursor = conn.cursor()


def if_not_empty_return_else_null(value):
    if value == '':
        return None
    return value


if __name__ == '__main__':
    with open('../data/koatuu.json') as f:
        arr = load(f)
        for obj in arr:
            key_arr = ["Перший рівень", "Другий рівень", "Третій рівень", "Четвертий рівень", "Категорія",
                       "Назва об'єкта українською мовою"]
            value_arr = map(lambda key: if_not_empty_return_else_null(obj[key]), key_arr)
            cursor.execute('''INSERT INTO data_science.stage_zone.classification_ukraine_objects (first_level, second_level, third_level,
                                                                    fourth_level, category, object_name)
VALUES (%s, %s, %s, %s, %s, %s);
                ''', tuple(value_arr))

conn.commit()
cursor.close()
conn.close()
