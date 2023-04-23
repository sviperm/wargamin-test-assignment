import argparse
import json
import os
import re
from typing import Generator

import geoip2.database
import psycopg2
import psycopg2.extras
from apache_beam import DoFn, ParDo, Pipeline
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from dotenv import load_dotenv

from utils import get_conn_cursor, setup_loggers, safe_float_convert

load_dotenv()

basic_logger, skipped_logger = setup_loggers()


class ProcessEvent(DoFn):
    def process(self, element: str) -> Generator[tuple[str | float, ...] | None, None, None]:
        """
        Process input element and yield event data if "battle_id" or "session_id" is found.

        Args:
            element: Input string element.

        Returns:
            Yields a tuple with event type and event data if valid event is found.
        """
        event = re.findall(r'\{.+\}', element)
        if event:
            event_dict = json.loads(event[0])
        else:
            skipped_logger.warning(f"Skipped element: {element}")
            yield None

        event_name = event_dict['event_name']

        if event_name == 'multiplayer_battle_ended':
            yield (
                "battle",
                event_dict["battle_id"],
                event_dict["user_ip"],
                event_dict["user_server_region"],
                event_dict["server_version"],
                event_dict["player_name"],
                event_dict["event_name"],
                event_dict["event_timestamp"],
                event_dict["user_device_country"],
                event_dict["user_id"],
                event_dict["user_type"],
                event_dict["client_version"],
                event_dict["is_premium"],
                event_dict["platform"],
                event_dict["user_is_spender"],
                event_dict["outcome"],
                event_dict["ship_destroyed"],
            )
        elif event_name == 'session_started':
            yield (
                "session",
                event_dict["session_id"],
                event_dict["user_ip"],
                event_dict["user_server_region"],
                event_dict["server_version"],
                event_dict["player_name"],
                event_dict["login_attempt_id"],
                event_dict["event_name"],
                event_dict["event_timestamp"],
                event_dict["user_device_country"],
                event_dict["user_id"],
                event_dict["user_type"],
                event_dict["client_version"],
                event_dict["is_premium"],
                event_dict["platform"],
                event_dict["user_is_spender"],
            )
        elif event_name == 'in_app_purchase_log_server':
            yield (
                "in_app_purchase",
                event_dict["user_id"],
                event_dict["user_ip"],
                event_dict["user_server_region"],
                event_dict["server_version"],
                event_dict["player_name"],
                event_dict["event_name"],
                event_dict["event_timestamp"],
                event_dict["user_device_country"],
                event_dict["user_type"],
                event_dict["client_version"],
                event_dict["is_premium"],
                event_dict["platform"],
                event_dict["product_name"],
                safe_float_convert(event_dict["real_currency_amount"]),
                event_dict["real_currency_type"],
                safe_float_convert(event_dict["usd_cost"]),
                event_dict["user_is_spender"],
            )
        else:
            skipped_logger.warning(f"Skipped element: {element}")
            yield None


class EnrichEvent(DoFn):
    def get_user_country(self, user_ip: str) -> tuple[str | None, str | None, float | None, float | None]:
        """
        Get the country name from the user's IP address.

        Args:
            user_ip: The user's IP address.

        Returns:
            The country name.
        """
        try:
            with geoip2.database.Reader('config/GeoLite2-City_20230421/GeoLite2-City.mmdb') as reader:
                response = reader.city(user_ip)
                country = response.country.name
                city = response.city.name
                latitude = response.location.latitude
                longitude = response.location.longitude
                return country, city, latitude, longitude
        except Exception as e:
            # basic_logger.warning(f"Failed to get country for IP {user_ip}: {e}")
            return None, None, None, None

    def process(self, element: tuple[str | float, ...] | None) -> Generator[tuple[str | float | None, ...] | None, None, None]:
        if not element:
            return None

        user_ip = element[2]
        geo_data = self.get_user_country(user_ip)  # type: ignore
        new_element = element + geo_data
        yield new_element


class WriteToPostgreSQL(DoFn):
    def __init__(self, batch_size: int = 1000):
        """
        Initialize WriteToPostgreSQL instance.

        Args:
            batch_size: Number of events to batch insert.
        """
        self.db_config = {
            'dbname': os.getenv('POSTGRES_DB'),
            'user': os.getenv('POSTGRES_USER'),
            'password': os.getenv('POSTGRES_PASSWORD'),
            'host': os.getenv('POSTGRES_HOST'),
            'port': os.getenv('POSTGRES_PORT'),
        }
        self.batch_size = batch_size

        self.battle_buffer: list[tuple[str | float | None, ...]]
        self.session_buffer: list[tuple[str | float | None, ...]]
        self.in_app_purchase_buffer: list[tuple[str | float | None, ...]]

    def start_bundle(self):
        self.battle_buffer = []
        self.session_buffer = []
        self.in_app_purchase_buffer = []

    def process(self, element: tuple[str | float | None, ...] | None):
        """
        Process event data and store it in PostgreSQL.

        Args:
            element: Tuple with event type and event data.
        """
        if not element:
            return

        event_type = element[0]

        if event_type == "battle":
            self.battle_buffer.append(element[1:])
            if len(self.battle_buffer) >= self.batch_size:
                self.create_battles_table()
                self.insert_battles_to_db()

        elif event_type == "session":
            self.session_buffer.append(element[1:])
            if len(self.session_buffer) >= self.batch_size:
                self.create_sessions_table()
                self.insert_sessions_to_db()

        elif event_type == "in_app_purchase":
            self.in_app_purchase_buffer.append(element[1:])
            if len(self.session_buffer) >= self.batch_size:
                self.create_in_app_purchase_table()
                self.insert_in_app_purchase_to_db()

    def finish_bundle(self):
        """
        Finish bundle by inserting remaining events in the buffer into the database.
        """
        if self.battle_buffer:
            self.create_battles_table()
            self.insert_battles_to_db()

        if self.session_buffer:
            self.create_sessions_table()
            self.insert_sessions_to_db()

        if self.in_app_purchase_buffer:
            self.create_in_app_purchase_table()
            self.insert_in_app_purchase_to_db()

    def create_battles_table(self):
        """
        Create 'battle_events' table in the database if it doesn't exist.
        """
        conn, cursor = get_conn_cursor(self.db_config)

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS battle_events (
                id SERIAL PRIMARY KEY,
                battle_id TEXT NOT NULL,
                user_ip TEXT,
                user_server_region TEXT,
                server_version TEXT,
                player_name TEXT,
                event_name TEXT,
                event_timestamp TIMESTAMP,
                user_device_country TEXT,
                user_id TEXT NOT NULL,
                user_type TEXT,
                client_version TEXT,
                is_premium BOOLEAN,
                platform TEXT,
                user_is_spender BOOLEAN,
                outcome TEXT,
                ship_destroyed INTEGER,
                country TEXT, 
                city TEXT, 
                latitude FLOAT, 
                longitude FLOAT
            )
        ''')

        conn.commit()
        conn.close()

    def create_sessions_table(self):
        """
        Create 'session_events' table in the database if it doesn't exist.
        """
        conn, cursor = get_conn_cursor(self.db_config)

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS session_events (
                id SERIAL PRIMARY KEY,
                session_id TEXT NOT NULL,
                user_ip TEXT,
                user_server_region TEXT,
                server_version TEXT,
                player_name TEXT,
                login_attempt_id TEXT,
                event_name TEXT,
                event_timestamp TIMESTAMP,
                user_device_country TEXT,
                user_id TEXT NOT NULL,
                user_type TEXT,
                client_version TEXT,
                is_premium BOOLEAN,
                platform TEXT,
                user_is_spender BOOLEAN,
                country TEXT, 
                city TEXT, 
                latitude FLOAT, 
                longitude FLOAT
            )
        ''')

        conn.commit()
        conn.close()

    def create_in_app_purchase_table(self):
        """
        Create 'in_app_purchase_events' table in the database if it doesn't exist.
        """
        conn, cursor = get_conn_cursor(self.db_config)

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS in_app_purchase_events (
                id SERIAL PRIMARY KEY,
                user_ip TEXT,
                user_server_region TEXT,
                server_version TEXT,
                player_name TEXT,
                event_name TEXT,
                event_timestamp TIMESTAMP,
                user_device_country TEXT,
                user_id TEXT,
                user_type TEXT,
                client_version TEXT,
                is_premium BOOL,
                platform TEXT,
                product_name TEXT,
                real_currency_amount FLOAT,
                real_currency_type TEXT,
                usd_cost FLOAT,
                user_is_spender BOOL,
                country TEXT, 
                city TEXT, 
                latitude FLOAT, 
                longitude FLOAT
            )
        ''')

        conn.commit()
        conn.close()

    def insert_battles_to_db(self):
        """
        Insert buffered battle events into the 'battle_events' table in the database.
        """
        conn, cursor = get_conn_cursor(self.db_config)

        psycopg2.extras.execute_batch(cursor, '''
            INSERT INTO battle_events (
                battle_id,
                user_ip,
                user_server_region,
                server_version,
                player_name,
                event_name,
                event_timestamp,
                user_device_country,
                user_id,
                user_type,
                client_version,
                is_premium,
                platform,
                user_is_spender,
                outcome,
                ship_destroyed,
                country,
                city,
                latitude,
                longitude
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', self.battle_buffer)

        conn.commit()
        conn.close()

        self.battle_buffer.clear()

    def insert_sessions_to_db(self):
        """
        Insert buffered session events into the 'session_events' table in the database.
        """
        conn, cursor = get_conn_cursor(self.db_config)

        psycopg2.extras.execute_batch(cursor, '''
            INSERT INTO session_events (
                session_id,
                user_ip,
                user_server_region,
                server_version,
                player_name,
                login_attempt_id,
                event_name,
                event_timestamp,
                user_device_country,
                user_id,
                user_type,
                client_version,
                is_premium,
                platform,
                user_is_spender,
                country,
                city,
                latitude,
                longitude
            )
            VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', self.session_buffer)

        conn.commit()
        conn.close()

        self.session_buffer.clear()

    def insert_in_app_purchase_to_db(self):
        """
        Insert buffered session events into the 'in_app_purchase_events' table in the database.
        """
        conn, cursor = get_conn_cursor(self.db_config)

        psycopg2.extras.execute_batch(cursor, '''
            INSERT INTO in_app_purchase_events (
                user_id,
                user_ip,
                user_server_region,
                server_version,
                player_name,
                event_name,
                event_timestamp,
                user_device_country,
                user_type,
                client_version,
                is_premium,
                platform,
                product_name,
                real_currency_amount,
                real_currency_type,
                usd_cost,
                user_is_spender,
                country,
                city,
                latitude,
                longitude
            )
            VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', self.in_app_purchase_buffer)

        conn.commit()
        conn.close()

        self.session_buffer.clear()


def run(argv: list[str] | None = None, save_main_session: bool = True) -> None:
    """
    Execute the ETL pipeline to process, enrich, and store event data in PostgreSQL.

    Args:
        argv: A list of command-line arguments, including input file path and batch size.
        save_main_session: A boolean value to determine whether to save the main session.
                           If True, the main session is saved, enabling pickling of global
                           variables.

    Returns:
        None
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='gs://wg_test_assignment/events.json',
        help='Input file to process.')
    parser.add_argument(
        '--batch_size',
        dest='batch_size',
        default=1000,
        help='Output file to write results to.')

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with Pipeline(options=pipeline_options) as p:
        events = (
            p
            | 'ReadFromGCS' >> ReadFromText(known_args.input)
            | 'ProcessEvent' >> ParDo(ProcessEvent())
            | 'EnrichEvent' >> ParDo(EnrichEvent())
            | 'Write to PostgreSQL' >> ParDo(WriteToPostgreSQL(batch_size=known_args.batch_size))
        )


if __name__ == '__main__':
    basic_logger.info("Starting ETL pipeline...")
    run()
    basic_logger.info("ETL pipeline completed.")
