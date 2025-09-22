from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.base import PokeReturnValue
import requests
import pendulum


@dag(
    dag_id="flights_above_me",
    schedule="*/15 * * * *",  # run every 15 minutes
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["flights", "opensky"],
)

def flights_above_me():
    # Step 1: Create schema tables
    create_tables = SQLExecuteQueryOperator(
        task_id="create_tables",
        conn_id="postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS user_location (
            location_id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            lat NUMERIC NOT NULL,
            lon NUMERIC NOT NULL,
            radius_km NUMERIC NOT NULL DEFAULT 30
        );

        CREATE TABLE IF NOT EXISTS aircraft (
            icao24 TEXT PRIMARY KEY,
            manufacturer TEXT,
            model TEXT,
            country TEXT,
            operator TEXT
        );

        CREATE TABLE IF NOT EXISTS flights (
            flight_id TEXT PRIMARY KEY,
            icao24 TEXT REFERENCES aircraft(icao24),
            callsign TEXT,
            dep_airport TEXT,
            arr_airport TEXT,
            dep_time TIMESTAMPTZ,
            arr_time TIMESTAMPTZ,
            source TEXT DEFAULT 'opensky'
        );

        CREATE TABLE IF NOT EXISTS state_vectors (
            state_id SERIAL PRIMARY KEY,
            icao24 TEXT,
            callsign TEXT,
            lat NUMERIC,
            lon NUMERIC,
            altitude NUMERIC,
            velocity NUMERIC,
            heading NUMERIC,
            timestamp TIMESTAMPTZ NOT NULL,
            raw_json JSONB
        );

        CREATE TABLE IF NOT EXISTS nearby_flights (
            nf_id SERIAL PRIMARY KEY,
            location_id INT REFERENCES user_location(location_id),
            icao24 TEXT,
            flight_id TEXT REFERENCES flights(flight_id),
            timestamp TIMESTAMPTZ NOT NULL,
            distance_km NUMERIC,
            is_overhead BOOLEAN DEFAULT FALSE,
            metadata JSONB
        );
        """,
    )

    # Step 2: Sensor to check OpenSky availability
    @task.sensor(poke_interval=60, timeout=600)
    def is_opensky_available() -> PokeReturnValue:
        url = "https://opensky-network.org/api/states/all"
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
        except requests.RequestException:
            return PokeReturnValue(is_done=False, xcom_value=None)

        data = response.json()
        return PokeReturnValue(is_done=("states" in data), xcom_value=data)

    # Step 3: Hard-coded user location (later can come from DB)
    @task(default_args={
        "retries": 2 
    }) 
    def get_location_config():
        return {
            "location_id": 1,
            "name": "Home",
            "lat": 53.55,
            "lon": -2.78,
            "radius_km": 30,
        }

    # Step 4: Extract states from API JSON
    @task(default_args={
        "retries": 2 
    }) 
    def extract_states(opensky_json):
        if not opensky_json:
            print("No data received from sensor")
            return []

        states = opensky_json.get("states", [])
        print(f"Extracted {len(states)} states from OpenSky API")
        return states
        
    # Step 5: Clean states - flatten and filter the raw states data
    @task(default_args={
        "retries": 2 
    }) 
    def clean_states(states):
        if not states:
            print("No states to clean")
            return []
        
        cleaned_states = []

        for state in states:
            # Flatten state into a dictionary with relevant fields
            cleaned_state = {
                "icao24": state[0],
                "callsign": state[1],
                "lat": state[6],
                "lon": state[5],
                "altitude": state[7],
                "velocity": state[9],
                "heading": state[10],
                "timestamp": pendulum.from_timestamp(state[3], tz="UTC"),
                "on_ground": state[8],
            }

            # Filter out invalid states (null lat/lon or on_ground == True)
            if None not in [cleaned_state["lat"], cleaned_state["lon"]] and not cleaned_state["on_ground"]:
                cleaned_states.append(cleaned_state)

        print(f"Cleaned {len(cleaned_states)} states")
        return cleaned_states


    # Dependencies
    location = get_location_config()
    api_data = is_opensky_available()
    states = extract_states(api_data)
    cleaned_states = clean_states(states)

    create_tables >> location
    location >> api_data >> states >> cleaned_states


flights_above_me()
