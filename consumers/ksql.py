"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests
import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"


KSQL_STATEMENT = """
CREATE TABLE turnstile(
    station_id INTEGER,
    station_name VARCHAR,
    line VARCHAR
) WITH (
    KAFKA_TOPIC='turnstile',
    VALUE_FORMAT='avro',
    KEY='station_id'
);

CREATE TABLE turnstile_summary
WITH (VALUE_FORMAT='json') AS 
    SELECT station_id, count(station_id) AS count
    FROM turnstile
    GROUP BY station_id
;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("turnstile_summary") is True:
        return
    
    logging.debug("executing ksql statement...")
    headers = {
        "Content-Type": "application/vnd.ksql.v1+json",
        "Accept": "application/vnd.ksql.v1+json"
    }
    stream_properties = {"ksql.streams.auto.offset.reset": "earliest"}
    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers=headers,
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": stream_properties,
            }
        )
    )
    # Ensure that a 2XX status code was returned
    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError as err:
        print(f"error message: {err}")


if __name__ == "__main__":
    execute_statement()