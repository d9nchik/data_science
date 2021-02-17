from psycopg2 import connect
from datetime import date

conn = connect(dbname='data_science', user='d9nich', password="J'~3)Yq=JpFAzQMT", host='localhost')
cursor = conn.cursor()


def remove_quotes(item):
    if len(item) > 0 and item[0] == '"':
        item = item[1:-1]
    return item


def if_empty_return_none(item):
    return None if item == '' else item


def convert_to_int_if_not_none(item):
    if item is not None:
        return int(item)
    return item


if __name__ == '__main__':
    with open('../data/tz_2020.csv') as f:
        counter = 0
        for line in f:
            counter += 1
            if counter == 1:
                continue
            arr = list(map(if_empty_return_none, map(remove_quotes, line.rstrip().split(';')[1:])))
            arr[1] = convert_to_int_if_not_none(arr[1])
            arr[4] = convert_to_int_if_not_none(arr[4])
            arr[8] = convert_to_int_if_not_none(arr[8])
            arr[14] = convert_to_int_if_not_none(arr[14])
            arr[15] = None if arr[15] == None else float(arr[15].replace(',', '.'))
            arr[16] = None if arr[16] == None else float(arr[16].replace(',', '.'))
            # extracting date
            date_parts = list(map(lambda x: int(x), arr[3].split('.')))
            arr[3] = date(date_parts[-1], date_parts[-2], date_parts[-3])
            #
            # print(arr)
            #
            # if counter==5:
            #     break

            # execute SQL
            try:
                cursor.execute('''INSERT INTO data_science.stage_zone.registered_cars (classification_ukraine_object_id, operation_code, operation_name,
                                                     registration_date, dep_code, dep, brand, model, make_year, color,
                                                     kind, body, purpose, fuel, capacity, own_weight, total_weight,
                                                     registration_number_new)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', tuple(arr))
            except Exception:
                print(arr)
                raise Exception

conn.commit()
cursor.close()
conn.close()
