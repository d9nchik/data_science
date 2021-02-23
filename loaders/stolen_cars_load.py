from json import load
from psycopg2 import connect
from datetime import datetime


def convert_string_to_time(string: str):
    date, time = string.split('T')
    year, month, day = map(int, date.split('-'))
    hours, minutes, seconds = map(int, time.split(':'))
    return datetime(year, month, day, hours, minutes, seconds)


def if_string_empty_return_none(value):
    return None if value == '' else value


if __name__ == '__main__':
    with connect(dbname='data_science', user='d9nich',
                 password="J'~3)Yq=JpFAzQMT", host='localhost') as conn:
        with conn.cursor() as cursor:
            with open('../data/carswanted.json', encoding='utf-8-sig') as f:
                data = load(f)
                for obj in data:
                    keys = ['brand', 'model', 'cartype', 'color', 'vehiclenumber', 'bodynumber', 'chassisnumber',
                            'enginenumber', 'illegalseizuredate', 'organunit', 'insertdate']
                    try:
                        obj['brand'], obj['model'] = obj['brandmodel'].split(' - ')
                        if obj['brand'] == '' or obj['model'] == '':
                            continue
                        obj['illegalseizuredate'] = convert_string_to_time(obj['illegalseizuredate'])
                        obj['insertdate'] = convert_string_to_time(obj['insertdate'])
                        values = list(map(if_string_empty_return_none, map(lambda key: obj[key], keys)))
                        # print(values)
                        # print(len(values))
                        cursor.execute(
                            '''INSERT INTO data_science.stage_zone.stolen_cars (brand, model, car_type, color, vehicle_number, body_number,
                                                             chassis_number, engine_number, illegal_seizure_date, organ_unit,
                                                             insert_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);''', tuple(values))
                    except ValueError:
                        continue
                conn.commit()

    print('done')
