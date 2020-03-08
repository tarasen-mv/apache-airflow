create table weather (
    station_id int,
    record_date date,
    temperature float,
    temperature_min float,
    temperature_max float,
    winddirection float,
    windspeed float,
    sunshine float,
    pressure float,
    average float,
    primary key (station_id, record_date)
);