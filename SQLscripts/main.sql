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

-- Maybe useless stuff
-- CREATE INDEX ON stage_zone.classification_ukraine_objects (first_level);
-- CREATE INDEX ON stage_zone.classification_ukraine_objects (second_level);
-- CREATE INDEX ON stage_zone.classification_ukraine_objects (third_level);
-- CREATE INDEX ON stage_zone.classification_ukraine_objects (fourth_level);

CREATE TABLE stage_zone.registered_cars
(
    registered_car_id                serial primary key not null,
    classification_ukraine_object_id char(10),
--     operation_code can be smallint
    operation_code                   int                not null,
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


EXPLAIN
SELECT distinct on (registered_car_id) *
FROM stage_zone.registered_cars rc
         JOIN filter_zone.classification_ukraine_objects cuo ON rc.classification_ukraine_object_id = cuo.level
         JOIN stage_zone.cars_descriptions cd
              ON rc.model = cd.model AND rc.brand = cd.brand AND rc.make_year > cd.year_production_putting;

