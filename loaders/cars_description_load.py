from json import loads
import psycopg2

keySequence = [
    "Brand",
    "Model",
    "Generation",
    "Modification (Engine)",
    "Doors",
    "Power",
    "Maximum speed",
    "Acceleration 0 - 100 km/h",
    "Year of putting into production",
    "Coupe type",
    "Seats",
    "Length",
    "Width",
    "Height",
    "Wheelbase",
    "Front track",
    "Rear (Back) track",
    "Minimum volume of Luggage (trunk)",
    "Position of engine",
    "Engine displacement",
    "Torque",
    "Turbine",
    "Position of cylinders",
    "Number of cylinders",
    "Cylinder Bore",
    "Piston Stroke",
    "Compression ratio",
    "Number of valves per cylinder",
    "Fuel Type",
    "Drive wheel",
    "Number of Gears (automatic transmission)",
    'is_transmission_automatic',
    "Front suspension",
    "Rear suspension",
    "ABS",
    "Steering type",
    "Power steering",
    "Minimum turning circle (turning diameter)",
    "Fuel consumption (economy) - urban",
    "Fuel consumption (economy) - extra urban",
    "Fuel consumption (economy) - combined",
    "Emission standard",
    "CO2 emissions",
    "Kerb Weight",
    "Fuel tank volume",
    "Width including mirrors",
    "Front overhang",
    "Rear overhang",
    "Fuel System",
    "Front brakes",
    "Rear brakes",
    "Permitted trailer load with brakes (12%)",
    "Tire size",
    "Wheel rims size",
    "Maximum volume of Luggage (trunk)",
    "Max weight",
    "Year of stopping production",
    "Valvetrain",
    "Ride height",
    "Model Engine",
    "Drag coefficient",
    "Maximum engine speed",
    "Approach angle",
    "Departure angle",
    "Electric motor power",
    "Electric motor Torque",
    "ICE power",
    "ICE Torque",
    "Engine oil capacity",
    "Coolant",
    "Permitted trailer load without brakes",
    "100 km/h - 0",
    "Max roof load",
    "Permitted towbar download",
    "Wading depth",
    "Permitted trailer load with brakes (8%)",
    "Battery capacity",
    "All-electric range",
    "Width with mirrors folded",
    "CNG cylinder capacity",
    "Fuel consumption (economy), urban - CNG",
    "Fuel consumption (economy), extra urban - CNG",
    "Fuel consumption (economy), combined  - CNG",
    "CNG CO2 emissions",
    "AdBlue tank",
    "Ramp angle",
    "Climb angle",
    "Acceleration 0 - 200 km/h",
    "Average Energy consumption",
    "Acceleration 0 - 300 km/h",
    "200 km/h - 0"
]

conn = psycopg2.connect(dbname='data_science', user='d9nich',
                        password="J'~3)Yq=JpFAzQMT", host='localhost')
cursor = conn.cursor()


def if_key_exist_split_item_and_convert_to_int(key, obj):
    if key in obj:
        obj[key] = int(obj[key].split()[0])


def if_key_exist_split_item_and_convert_to_float(key, obj):
    if key in obj:
        obj[key] = float(obj[key].split()[0])


if __name__ == '__main__':
    with open('../data/car_database.json') as f:
        for line in f:
            obj = loads(line)

            if_key_exist_split_item_and_convert_to_int("Year of putting into production", obj)
            if_key_exist_split_item_and_convert_to_int("Engine displacement", obj)

            key_number_of_cylinders = "Number of cylinders"
            if key_number_of_cylinders in obj:
                obj[key_number_of_cylinders] = int(obj[key_number_of_cylinders])

            if_key_exist_split_item_and_convert_to_float("Cylinder Bore", obj)
            if_key_exist_split_item_and_convert_to_float("Piston Stroke", obj)

            key_compression_ratio = "Compression ratio"
            if key_compression_ratio in obj:
                obj[key_compression_ratio] = float(obj[key_compression_ratio])

            key_number_of_valves_per_cylinder = "Number of valves per cylinder"
            if key_number_of_valves_per_cylinder in obj:
                obj[key_number_of_valves_per_cylinder] = int(obj[key_number_of_valves_per_cylinder])

            obj['is_transmission_automatic'] = "Number of Gears (automatic transmission)" in obj

            key_number_of_gears_manual = "Number of Gears (manual transmission)"

            if key_number_of_gears_manual in obj:
                obj["Number of Gears (automatic transmission)"] = obj[key_number_of_gears_manual]

            key_is_abs = "ABS"
            if key_is_abs in obj:
                obj[key_is_abs] = 'true' == obj[key_is_abs]

            if_key_exist_split_item_and_convert_to_int("Width including mirrors", obj)
            if_key_exist_split_item_and_convert_to_int("Front overhang", obj)
            if_key_exist_split_item_and_convert_to_int("Year of stopping production", obj)

            if_key_exist_split_item_and_convert_to_float("Engine oil capacity", obj)
            if_key_exist_split_item_and_convert_to_float("Coolant", obj)
            if_key_exist_split_item_and_convert_to_float("100 km/h - 0", obj)
            if_key_exist_split_item_and_convert_to_float("Battery capacity", obj)

            if_key_exist_split_item_and_convert_to_int("Width with mirrors folded", obj)

            if_key_exist_split_item_and_convert_to_float("CNG cylinder capacity", obj)
            if_key_exist_split_item_and_convert_to_float("AdBlue tank", obj)

            key_climb_angle = "Climb angle"
            if key_climb_angle in obj:
                obj[key_climb_angle] = obj[key_climb_angle][:-1]

            if_key_exist_split_item_and_convert_to_float("200 km/h - 0", obj)

            result = map(lambda key: obj.get(key, None), keySequence)

            cursor.execute('''INSERT INTO data_science.stage_zone.cars_descriptions
(brand, model, generation, modification_engine, doors, power, max_speed, acceleration_0_100_km_h,
 year_production_putting, coupe_type, seats, length, width, height, wheelbase, front_track, rear_back_track,
 minimum_volume_of_luggage_trunk, position_of_engine, engine_displacement, torque, turbine,
 position_of_cylinders, number_of_cylinders, cylinders_bore, piston_stroke, compression_ratio,
 number_of_valves_per_cylinder, fuel_type, drive_wheel, number_of_gears, is_transmission_automatic,
 front_suspension, rear_suspension, is_abs, steering_type, power_steering,
 min_turning_circle_turning_diameter, fuel_consumption_urban,
 fuel_consumption_extra_urban, fuel_consumption_combined, emission_standard, co2_emission, kerb_weight,
 fuel_tank_volume, width_including_mirrors, front_overhang, rear_overhang, fuel_system, front_brakes,
 rear_brakes, permitted_trailer_load_with_brakes_12_percent, tire_size, wheel_rims_size,
 maximum_volume_of_luggage_trunk, max_weight, year_of_stopping_production, valvetrain, ride_height,
 model_engine, drag_coefficient, maximum_engine_speed, approach_angle, departure_angle,
 electric_motor_power, electric_motor_torque, ice_power, ice_torque, engine_oil_capacity,
 coolant, permitted_trailer_load_without_brakes, hundred_km_per_hour_0_distance_in_m,
 max_roof_load, permitted_towbar_download, wading_depth, permitted_load_with_brakes_8_percent,
 battery_capacity_kwh, all_electric_range, width_with_mirrors_folded, cng_cylinder_capacity,
 fuel_consumption_economy_urban_cng, fuel_consumption_economy_extra_urban_cng,
 fuel_consumption_economy_combined_urban_cng, cng_co2_emissions, adblue_tank, ramp_angle,
 climb_angle, acceleration_0_200_km_h, average_energy_consumption, acceleration_0_300_km_h,
 two_hundred_km_per_hour_0_distance_in_m)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s);

            ''', tuple(result))

conn.commit()
cursor.close()
conn.close()
