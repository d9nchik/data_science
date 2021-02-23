CREATE DATABASE data_science;

CREATE schema stage_zone;

CREATE TABLE stage_zone.classification_ukraine_objects
(
    classification_ukraine_object_id serial primary key not null,
    first_level                      char(10)           not null,
    second_level                     char(10),
    third_level                      char(10),
    fourth_level                     char(10),
    category                         char(1),
    object_name                      varchar(75)        not null
);

CREATE TABLE stage_zone.registered_cars
(
    registered_car_id                serial primary key not null,
    classification_ukraine_object_id char(10),
    operation_code                   smallint           not null,
    operation_name                   varchar(120)       not null,
    registration_date                date               not null,
    dep_code                         smallint           not null,
    dep                              varchar(110)       not null,
    brand                            varchar(30)        not null,
    model                            varchar(40)        not null,
    make_year                        smallint           not null,
    color                            varchar(15)        not null,
    kind                             varchar(15)        not null,
    body                             varchar(40)        not null,
    purpose                          varchar(20)        not null,
    fuel                             varchar(30),
    capacity                         int,
    own_weight                       double precision,
    total_weight                     double precision,
    registration_number_new          varchar(20)
);

CREATE TABLE stage_zone.stolen_cars
(
    stolen_cars_id       serial primary key not null,
    brand                varchar(30)        not null,
    model                varchar(30)        not null,
    car_type             varchar(40)        not null,
    color                varchar(15)        not null,
    vehicle_number       varchar(20),
    body_number          varchar(20),
    chassis_number       varchar(20),
    engine_number        varchar(20),
    illegal_seizure_date timestamp          not null,
    organ_unit           varchar(110)       not null,
    insert_date          timestamp          not null
);

CREATE TABLE stage_zone.cars_descriptions
(
    cars_descriptions_id                          serial primary key not null,
    brand                                         varchar(20)        not null,
    model                                         varchar(30)        not null,
    generation                                    varchar(50)        not null,
    modification_engine                           varchar(70)        not null,
    doors                                         varchar(3),
    power                                         varchar(35),
    max_speed                                     varchar(15),
    acceleration_0_100_km_h                       varchar(15),
    year_production_putting                       smallint,
    coupe_type                                    varchar(25),
    seats                                         varchar(5),
    length                                        varchar(20),
    width                                         varchar(15),
    height                                        varchar(15),
    wheelbase                                     varchar(15),
    front_track                                   varchar(15),
    rear_back_track                               varchar(20),
    minimum_volume_of_luggage_trunk               varchar(20),
    position_of_engine                            varchar(20),
    engine_displacement                           smallint,
    torque                                        varchar(45),
    turbine                                       varchar(55),
    position_of_cylinders                         varchar(20),
    number_of_cylinders                           smallint,
    cylinders_bore                                double precision,
    piston_stroke                                 double precision,
    compression_ratio                             double precision,
    number_of_valves_per_cylinder                 smallint,
    fuel_type                                     varchar(40)        not null,
    drive_wheel                                   varchar(25),
    number_of_gears                               varchar(35),
    is_transmission_automatic                     boolean            not null,
    front_suspension                              varchar(50),
    rear_suspension                               varchar(80),
    is_abs                                        bool,
    steering_type                                 varchar(35),
    power_steering                                varchar(20),
    min_turning_circle_turning_diameter           varchar(20),
    fuel_consumption_urban                        varchar(30),
    fuel_consumption_extra_urban                  varchar(25),
    fuel_consumption_combined                     varchar(25),
    emission_standard                             varchar(15),
    co2_emission                                  varchar(20),
    kerb_weight                                   varchar(25),
    fuel_tank_volume                              varchar(20),
    width_including_mirrors                       smallint,
    front_overhang                                smallint,
    rear_overhang                                 varchar(15),
    fuel_system                                   varchar(40),
    front_brakes                                  varchar(16),
    rear_brakes                                   varchar(16),
    permitted_trailer_load_with_brakes_12_percent varchar(15),
    tire_size                                     varchar(100),
    wheel_rims_size                               varchar(85),
    maximum_volume_of_luggage_trunk               varchar(15),
    max_weight                                    varchar(15),
    year_of_stopping_production                   smallint,
    valvetrain                                    varchar(20),
    ride_height                                   varchar(15),
    model_engine                                  varchar(30),
    drag_coefficient                              varchar(15),
    maximum_engine_speed                          varchar(15),
    approach_angle                                varchar(25),
    departure_angle                               varchar(30),
    electric_motor_power                          varchar(55),
    electric_motor_torque                         varchar(60),
    ice_power                                     varchar(25),
    ice_torque                                    varchar(25),
    engine_oil_capacity                           double precision,
    coolant                                       double precision,
    permitted_trailer_load_without_brakes         varchar(15),
    hundred_km_per_hour_0_distance_in_m           double precision,
    max_roof_load                                 varchar(15),
    permitted_towbar_download                     varchar(10),
    wading_depth                                  varchar(15),
    permitted_load_with_brakes_8_percent          varchar(15),
    battery_capacity_kwh                          double precision,
    all_electric_range                            varchar(15),
    width_with_mirrors_folded                     smallint,
    cng_cylinder_capacity                         double precision,
    fuel_consumption_economy_urban_cng            varchar(20),
    fuel_consumption_economy_extra_urban_cng      varchar(20),
    fuel_consumption_economy_combined_urban_cng   varchar(25),
    cng_co2_emissions                             varchar(15),
    adblue_tank                                   double precision,
    ramp_angle                                    varchar(15),
    climb_angle                                   smallint,
    acceleration_0_200_km_h                       varchar(10),
    average_energy_consumption                    varchar(20),
    acceleration_0_300_km_h                       varchar(10),
    two_hundred_km_per_hour_0_distance_in_m       double precision
);

CREATE INDEX lowercase_model_brand_car_index_of_cars_descriptions ON stage_zone.cars_descriptions (lower(brand), lower(model));

CREATE schema filter_zone;

CREATE TABLE filter_zone.classification_ukraine_objects
(
    classification_ukraine_object_id serial primary key not null,
    level                            char(10) unique    not null,
    category                         char(1),
    object_name                      varchar(75)        not null
);

CREATE INDEX ON filter_zone.classification_ukraine_objects (level);



CREATE OR REPLACE FUNCTION stage_zone.classification_ukrainian_objects_insert_trigger_fnc()
    RETURNS trigger AS
$$
BEGIN
    if NEW.fourth_level is not null then
        INSERT INTO filter_zone.classification_ukraine_objects (level, category, object_name)
        VALUES (NEW.fourth_level, new.category, new.object_name);
    elsif NEW.third_level is not null then
        INSERT INTO filter_zone.classification_ukraine_objects (level, category, object_name)
        VALUES (NEW.third_level, new.category, new.object_name);
    elsif NEW.second_level is not null then
        INSERT INTO filter_zone.classification_ukraine_objects (level, category, object_name)
        VALUES (NEW.second_level, new.category, new.object_name);
    else
        INSERT INTO filter_zone.classification_ukraine_objects (level, category, object_name)
        VALUES (NEW.first_level, new.category, new.object_name);
    END IF;
    RETURN NEW;
END;
$$
    LANGUAGE 'plpgsql';

CREATE TRIGGER classification_ukraine_objects_insert_trigger
    BEFORE INSERT
    ON stage_zone.classification_ukraine_objects

    FOR EACH ROW

EXECUTE PROCEDURE stage_zone.classification_ukrainian_objects_insert_trigger_fnc();

-- Main resolution

CREATE TABLE public.brand_dimension
(
    brand_dimension_id serial primary key not null,
    brand              varchar(30) unique not null
);

CREATE TABLE public.model_dimension
(
    model_dimension_id serial primary key not null,
    model              varchar(40) unique not null
);

CREATE TABLE public.color_dimension
(
    color_dimension_id serial primary key not null,
    color              varchar(15) unique not null
);

CREATE TABLE public.registration_date_dimension
(
    registration_date_dimension_id serial primary key not null,
    registration_date              date unique        not null
);

CREATE TABLE public.department_dimension
(
    department_dimension_id serial primary key  not null,
    department_code         smallint unique     not null,
    department              varchar(110) unique not null
);

CREATE TABLE public.operation_dimension
(
    operation_dimension_id serial primary key  not null,
    operation_code         smallint unique     not null,
    operation_name         varchar(120) unique not null
);

CREATE TABLE public.classification_ukraine_object_dimension
(
    classification_ukraine_object_dimension_id serial primary key not null,
    level                                      char(10) unique    not null,
    category                                   char(1),
    object_name                                varchar(75)        not null
);



CREATE TABLE public.make_year_dimension
(
    make_year_dimension_id serial primary key not null,
    make_year              smallint unique    not null
);

CREATE TABLE public.engine_dimension
(
    engine_dimension_id serial primary key not null,
    engine_type         varchar(70) unique not null
);

CREATE TABLE public.fuel_type_dimension
(
    fuel_type_dimension_id serial primary key not null,
    fuel_type              varchar(40)        not null unique
);

CREATE TABLE public.detail_information_about_car_registration
(
    detail_information_about_car_registration_id  serial      not null primary key,
    brand_dimension_id                            int         not null
        constraint detail_information_about_car_brand_dimension_id_fk references brand_dimension
            on update cascade on DELETE restrict,
    classification_ukraine_object_dimension_id    int
        constraint detail_information_about_car_cuo_dimension_id_fk references classification_ukraine_object_dimension
            on update cascade on DELETE restrict,
    color_dimension_id                            int         not null
        constraint detail_information_about_car_color_dimension_id_fk references color_dimension
            on update cascade on DELETE restrict,
    department_dimension_id                       int         not null
        constraint detail_information_about_car_department_dimension_id_fk references department_dimension
            on update cascade on DELETE restrict,
    engine_dimension_id                           int
        constraint detail_information_about_car_engine_dimension_id_fk references engine_dimension
            on update cascade on DELETE restrict,
    fuel_type_dimension_id                        int
        constraint detail_information_about_car_fuel_type_dimension_id_fk references fuel_type_dimension
            on update cascade on DELETE restrict,
    make_year_dimension_id                        int         not null
        constraint detail_information_about_car_make_year_dimension_id_fk references make_year_dimension
            on update cascade on DELETE restrict,
    model_dimension_id                            int         not null
        constraint detail_information_about_car_model_dimension_id_fk references model_dimension
            on update cascade on DELETE restrict,
    operation_dimension_id                        int         not null
        constraint detail_information_about_car_operation_dimension_id_fk references operation_dimension
            on update cascade on DELETE restrict,
    registration_date_dimension_id                int         not null
        constraint detail_information_about_car_registration_date_dimension_id_fk references registration_date_dimension
            on update cascade on DELETE restrict,
    generation                                    varchar(50) not null,
    doors                                         varchar(3),
    power                                         varchar(35),
    max_speed                                     varchar(15),
    acceleration_0_100_km_h                       varchar(15),
    year_production_putting                       smallint,
    coupe_type                                    varchar(25),
    seats                                         varchar(5),
    length                                        varchar(20),
    width                                         varchar(15),
    height                                        varchar(15),
    wheelbase                                     varchar(15),
    front_track                                   varchar(15),
    rear_back_track                               varchar(20),
    minimum_volume_of_luggage_trunk               varchar(20),
    position_of_engine                            varchar(20),
    engine_displacement                           smallint,
    torque                                        varchar(45),
    turbine                                       varchar(55),
    position_of_cylinders                         varchar(20),
    number_of_cylinders                           smallint,
    cylinders_bore                                double precision,
    piston_stroke                                 double precision,
    compression_ratio                             double precision,
    number_of_valves_per_cylinder                 smallint,
    drive_wheel                                   varchar(25),
    number_of_gears                               varchar(35),
    front_suspension                              varchar(50),
    rear_suspension                               varchar(80),
    is_abs                                        bool,
    steering_type                                 varchar(35),
    power_steering                                varchar(20),
    min_turning_circle_turning_diameter           varchar(20),
    fuel_consumption_urban                        varchar(30),
    fuel_consumption_extra_urban                  varchar(25),
    fuel_consumption_combined                     varchar(25),
    emission_standard                             varchar(15),
    co2_emission                                  varchar(20),
    kerb_weight                                   varchar(25),
    fuel_tank_volume                              varchar(20),
    width_including_mirrors                       smallint,
    front_overhang                                smallint,
    rear_overhang                                 varchar(15),
    fuel_system                                   varchar(40),
    front_brakes                                  varchar(16),
    rear_brakes                                   varchar(16),
    permitted_trailer_load_with_brakes_12_percent varchar(15),
    tire_size                                     varchar(100),
    wheel_rims_size                               varchar(85),
    maximum_volume_of_luggage_trunk               varchar(15),
    max_weight                                    varchar(15),
    year_of_stopping_production                   smallint,
    valvetrain                                    varchar(20),
    ride_height                                   varchar(15),
    model_engine                                  varchar(30),
    drag_coefficient                              varchar(15),
    maximum_engine_speed                          varchar(15),
    approach_angle                                varchar(25),
    departure_angle                               varchar(30),
    electric_motor_power                          varchar(55),
    electric_motor_torque                         varchar(60),
    ice_power                                     varchar(25),
    ice_torque                                    varchar(25),
    engine_oil_capacity                           double precision,
    coolant                                       double precision,
    permitted_trailer_load_without_brakes         varchar(15),
    hundred_km_per_hour_0_distance_in_m           double precision,
    max_roof_load                                 varchar(15),
    permitted_towbar_download                     varchar(10),
    wading_depth                                  varchar(15),
    permitted_load_with_brakes_8_percent          varchar(15),
    battery_capacity_kwh                          double precision,
    all_electric_range                            varchar(15),
    width_with_mirrors_folded                     smallint,
    cng_cylinder_capacity                         double precision,
    fuel_consumption_economy_urban_cng            varchar(20),
    fuel_consumption_economy_extra_urban_cng      varchar(20),
    fuel_consumption_economy_combined_urban_cng   varchar(25),
    cng_co2_emissions                             varchar(15),
    adblue_tank                                   double precision,
    ramp_angle                                    varchar(15),
    climb_angle                                   smallint,
    acceleration_0_200_km_h                       varchar(10),
    average_energy_consumption                    varchar(20),
    acceleration_0_300_km_h                       varchar(10),
    two_hundred_km_per_hour_0_distance_in_m       double precision
);

-- Let's do some automation

CREATE OR REPLACE FUNCTION public.get_or_create_brand_id(brand_name varchar) returns int AS
$$
BEGIN
    INSERT INTO public.brand_dimension (brand)
    SELECT brand_name
    WHERE NOT EXISTS(SELECT NULL -- canonical way, but you can select
                            -- anything as EXISTS only checks existence
                     FROM public.brand_dimension
                     WHERE brand = brand_name
        );
    RETURN (SELECT brand_dimension_id FROM brand_dimension WHERE brand = brand_name);
end;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.get_or_create_classification_ukraine_object_id(level_search char) returns int AS
$$
BEGIN
    if level_search is null then
        RETURN null;
    end if;
    INSERT INTO public.classification_ukraine_object_dimension (level, category, object_name)
    SELECT level, category, object_name
    FROM filter_zone.classification_ukraine_objects
    WHERE level = level_search
      AND NOT EXISTS(SELECT NULL -- canonical way, but you can select
                            -- anything as EXISTS only checks existence
                     FROM public.classification_ukraine_object_dimension
                     WHERE level_search = classification_ukraine_object_dimension.level
        );
    RETURN (SELECT classification_ukraine_object_dimension_id
            FROM public.classification_ukraine_object_dimension
            WHERE level = level_search);
end;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.get_or_create_color_id(color_name varchar) returns int AS
$$
BEGIN
    INSERT INTO public.color_dimension (color)
    SELECT color_name
    WHERE NOT EXISTS(SELECT NULL -- canonical way, but you can select
                            -- anything as EXISTS only checks existence
                     FROM public.color_dimension
                     WHERE color = color_name
        );
    RETURN (SELECT color_dimension_id
            FROM public.color_dimension
            WHERE color = color_name);
end;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.get_or_create_department_id(department_cod smallint, department varchar) returns int AS
$$
BEGIN
    INSERT INTO public.department_dimension (department_code, department)
    SELECT department_cod,
           department
    WHERE NOT EXISTS(SELECT NULL -- canonical way, but you can select
                            -- anything as EXISTS only checks existence
                     FROM public.department_dimension
                     WHERE department_cod = department_dimension.department_code
        );
    RETURN (SELECT department_dimension_id
            FROM public.department_dimension
            WHERE department_code = department_cod);
end;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.get_or_create_engine_id(engine_name varchar) returns int AS
$$
BEGIN
    INSERT INTO public.engine_dimension (engine_type)
    SELECT engine_name
    WHERE NOT EXISTS(SELECT NULL -- canonical way, but you can select
                            -- anything as EXISTS only checks existence
                     FROM public.engine_dimension
                     WHERE engine_type = engine_name
        );
    RETURN (SELECT engine_dimension_id
            FROM public.engine_dimension
            WHERE engine_type = engine_name);
end;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.get_or_create_fuel_id(fuel_type_name varchar) returns int AS
$$
BEGIN
    INSERT INTO fuel_type_dimension (fuel_type)
    SELECT fuel_type_name
    WHERE NOT EXISTS(SELECT NULL -- canonical way, but you can select
                            -- anything as EXISTS only checks existence
                     FROM public.fuel_type_dimension
                     WHERE fuel_type = fuel_type_name
        );
    RETURN (SELECT fuel_type_dimension_id
            FROM public.fuel_type_dimension
            WHERE fuel_type = fuel_type_name);
end;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.get_or_create_make_year_id(year smallint) returns int AS
$$
BEGIN
    INSERT INTO public.make_year_dimension (make_year)
    SELECT year
    WHERE NOT EXISTS(SELECT NULL -- canonical way, but you can select
                            -- anything as EXISTS only checks existence
                     FROM public.make_year_dimension
                     WHERE make_year = year
        );
    RETURN (SELECT make_year_dimension_id
            FROM public.make_year_dimension
            WHERE make_year = year);
end;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.get_or_create_model_id(model_type varchar) returns int AS
$$
BEGIN
    INSERT INTO public.model_dimension (model)
    SELECT model_type
    WHERE NOT EXISTS(SELECT NULL -- canonical way, but you can select
                            -- anything as EXISTS only checks existence
                     FROM public.model_dimension
                     WHERE model = model_type
        );
    RETURN (SELECT model_dimension_id
            FROM public.model_dimension
            WHERE model = model_type);
end;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.get_or_create_operation_id(operation_cod smallint, operation_name varchar) returns int AS
$$
BEGIN
    INSERT INTO public.operation_dimension (operation_code, operation_name)
    SELECT operation_cod, operation_name
    WHERE NOT EXISTS(SELECT NULL -- canonical way, but you can select
                            -- anything as EXISTS only checks existence
                     FROM public.operation_dimension
                     WHERE operation_code = operation_cod
        );
    RETURN (SELECT operation_dimension_id
            FROM public.operation_dimension
            WHERE operation_code = operation_cod);
end;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.get_or_create_registration_date_id(dat date) returns int AS
$$
BEGIN
    INSERT INTO public.registration_date_dimension (registration_date)
    SELECT dat
    WHERE NOT EXISTS(SELECT NULL -- canonical way, but you can select
                            -- anything as EXISTS only checks existence
                     FROM public.registration_date_dimension
                     WHERE registration_date = dat
        );
    RETURN (SELECT registration_date_dimension_id
            FROM public.registration_date_dimension
            WHERE registration_date = dat);
end;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION stage_zone.registered_cars_insert_trigger_fnc()
    RETURNS trigger AS
$$
DECLARE
    car_id int := (SELECT cars_descriptions_id
                   FROM stage_zone.cars_descriptions
                   WHERE lower(brand) = lower(new.brand)
                     AND lower(model) = lower(new.model)
                     AND year_production_putting <= new.make_year
                   ORDER BY year_production_putting DESC
                   LIMIT 1);
BEGIN
    if car_id is not null then
        WITH description as (SELECT * FROM stage_zone.cars_descriptions WHERE cars_descriptions_id = car_id)
        INSERT
        INTO public.detail_information_about_car_registration (brand_dimension_id,
                                                               classification_ukraine_object_dimension_id,
                                                               color_dimension_id, department_dimension_id,
                                                               engine_dimension_id, fuel_type_dimension_id,
                                                               make_year_dimension_id, model_dimension_id,
                                                               operation_dimension_id,
                                                               registration_date_dimension_id, generation, doors,
                                                               power, max_speed, acceleration_0_100_km_h,
                                                               year_production_putting, coupe_type, seats,
                                                               length, width, height, wheelbase, front_track,
                                                               rear_back_track, minimum_volume_of_luggage_trunk,
                                                               position_of_engine, engine_displacement, torque,
                                                               turbine, position_of_cylinders,
                                                               number_of_cylinders, cylinders_bore,
                                                               piston_stroke, compression_ratio,
                                                               number_of_valves_per_cylinder, drive_wheel,
                                                               number_of_gears, front_suspension,
                                                               rear_suspension, is_abs, steering_type,
                                                               power_steering,
                                                               min_turning_circle_turning_diameter,
                                                               fuel_consumption_urban,
                                                               fuel_consumption_extra_urban,
                                                               fuel_consumption_combined, emission_standard,
                                                               co2_emission, kerb_weight, fuel_tank_volume,
                                                               width_including_mirrors, front_overhang,
                                                               rear_overhang, fuel_system, front_brakes,
                                                               rear_brakes,
                                                               permitted_trailer_load_with_brakes_12_percent,
                                                               tire_size, wheel_rims_size,
                                                               maximum_volume_of_luggage_trunk, max_weight,
                                                               year_of_stopping_production, valvetrain,
                                                               ride_height, model_engine, drag_coefficient,
                                                               maximum_engine_speed, approach_angle,
                                                               departure_angle, electric_motor_power,
                                                               electric_motor_torque, ice_power, ice_torque,
                                                               engine_oil_capacity, coolant,
                                                               permitted_trailer_load_without_brakes,
                                                               hundred_km_per_hour_0_distance_in_m,
                                                               max_roof_load, permitted_towbar_download,
                                                               wading_depth,
                                                               permitted_load_with_brakes_8_percent,
                                                               battery_capacity_kwh, all_electric_range,
                                                               width_with_mirrors_folded, cng_cylinder_capacity,
                                                               fuel_consumption_economy_urban_cng,
                                                               fuel_consumption_economy_extra_urban_cng,
                                                               fuel_consumption_economy_combined_urban_cng,
                                                               cng_co2_emissions, adblue_tank, ramp_angle,
                                                               climb_angle, acceleration_0_200_km_h,
                                                               average_energy_consumption,
                                                               acceleration_0_300_km_h,
                                                               two_hundred_km_per_hour_0_distance_in_m)
        VALUES ((SELECT * FROM public.get_or_create_brand_id(NEW.brand)),
                (SELECT *
                 FROM public.get_or_create_classification_ukraine_object_id(NEW.classification_ukraine_object_id)),
                (SELECT * FROM public.get_or_create_color_id(new.color)),
                (SELECT * FROm public.get_or_create_department_id(new.dep_code, new.dep)),
                (SELECT * FROm public.get_or_create_engine_id((SELECT modification_engine FROM description))),
                (SELECT * FROM public.get_or_create_fuel_id((SELECT fuel_type FROM description))),
                (SELECT * FROM public.get_or_create_make_year_id(new.make_year)),
                (SELECT * FROM public.get_or_create_model_id(new.model)),
                (SELECT * FROM public.get_or_create_operation_id(new.operation_code, new.operation_name)),
                (SELECT * FROM public.get_or_create_registration_date_id(new.registration_date)),
                (SELECT generation FROM description),
                (SELECT doors FROM description),
                (SELECT power FROM description),
                (SELECT max_speed FROM description),
                (SELECT acceleration_0_100_km_h FROM description),
                (SELECT year_production_putting FROM description),
                (SELECT coupe_type FROM description),
                (SELECT seats FROM description),
                (SELECT length FROM description),
                (SELECT width FROM description),
                (SELECT height FROM description),
                (SELECT wheelbase FROM description),
                (SELECT front_track FROM description),
                (SELECT rear_back_track FROM description),
                (SELECT minimum_volume_of_luggage_trunk FROM description),
                (SELECT position_of_engine FROM description),
                (SELECT engine_displacement FROM description),
                (SELECT torque FROM description),
                (SELECT turbine FROM description),
                (SELECT position_of_cylinders FROM description),
                (SELECT number_of_cylinders FROM description),
                (SELECT cylinders_bore FROM description),
                (SELECT piston_stroke FROM description),
                (SELECT compression_ratio FROM description),
                (SELECT number_of_valves_per_cylinder FROM description),
                (SELECT drive_wheel FROM description),
                (SELECT number_of_gears FROM description),
                (SELECT front_suspension FROM description),
                (SELECT rear_suspension FROM description),
                (SELECT is_abs FROM description),
                (SELECT steering_type FROM description),
                (SELECT power_steering FROM description),
                (SELECT min_turning_circle_turning_diameter FROM description),
                (SELECT fuel_consumption_urban FROM description),
                (SELECT fuel_consumption_extra_urban FROM description),
                (SELECT fuel_consumption_combined FROM description),
                (SELECT emission_standard FROM description),
                (SELECT co2_emission FROM description),
                (SELECT kerb_weight FROM description),
                (SELECT fuel_tank_volume FROM description),
                (SELECT width_including_mirrors FROM description),
                (SELECT front_overhang FROM description),
                (SELECT rear_overhang FROM description),
                (SELECT fuel_system FROM description),
                (SELECT front_brakes FROM description),
                (SELECT rear_brakes FROM description),
                (SELECT permitted_trailer_load_with_brakes_12_percent FROM description),
                (SELECT tire_size FROM description),
                (SELECT wheel_rims_size FROM description),
                (SELECT maximum_volume_of_luggage_trunk FROM description),
                (SELECT max_weight FROM description),
                (SELECT year_of_stopping_production FROM description),
                (SELECT valvetrain FROM description),
                (SELECT ride_height FROM description),
                (SELECT model_engine FROM description),
                (SELECT drag_coefficient FROM description),
                (SELECT maximum_engine_speed FROM description),
                (SELECT approach_angle FROM description),
                (SELECT departure_angle FROM description),
                (SELECT electric_motor_power FROM description),
                (SELECT electric_motor_torque FROM description),
                (SELECT ice_power FROM description),
                (SELECT ice_torque FROM description),
                (SELECT engine_oil_capacity FROM description),
                (SELECT coolant FROM description),
                (SELECT permitted_trailer_load_without_brakes FROM description),
                (SELECT hundred_km_per_hour_0_distance_in_m FROM description),
                (SELECT max_roof_load FROM description),
                (SELECT permitted_towbar_download FROM description),
                (SELECT wading_depth FROM description),
                (SELECT permitted_load_with_brakes_8_percent FROM description),
                (SELECT battery_capacity_kwh FROM description),
                (SELECT all_electric_range FROM description),
                (SELECT width_with_mirrors_folded FROM description),
                (SELECT cng_cylinder_capacity FROM description),
                (SELECT fuel_consumption_economy_urban_cng FROM description),
                (SELECT fuel_consumption_economy_extra_urban_cng FROM description),
                (SELECT fuel_consumption_economy_combined_urban_cng FROM description),
                (SELECT cng_co2_emissions FROM description),
                (SELECT adblue_tank FROM description),
                (SELECT ramp_angle FROM description),
                (SELECT climb_angle FROM description),
                (SELECT acceleration_0_200_km_h FROM description),
                (SELECT average_energy_consumption FROM description),
                (SELECT acceleration_0_300_km_h FROM description),
                (SELECT two_hundred_km_per_hour_0_distance_in_m FROM description));
    end if;
    RETURN NEW;
END;
$$
    LANGUAGE 'plpgsql';

CREATE TRIGGER registered_cars_insert_trigger
    BEFORE INSERT
    ON stage_zone.registered_cars

    FOR EACH ROW

EXECUTE PROCEDURE stage_zone.registered_cars_insert_trigger_fnc();


EXPLAIN
SELECT cars_descriptions_id
FROM stage_zone.cars_descriptions
WHERE lower(brand) = lower('BMW')
  AND lower(model) = lower('X5')
  AND year_production_putting <= 2009
ORDER BY year_production_putting DESC
LIMIT 1;
