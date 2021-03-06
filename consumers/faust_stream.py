"""Defines trends calculations for stations"""
import logging
import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str

app = faust.App(
    "stations-stream", 
    broker="kafka://localhost:9092", 
    store="memory://"
)
input_topic = app.topic("stations", value_type=Station) # given empty prefix
out_topic = app.topic("processed_stations", partitions=1)
table = app.Table(
    "processed_stations",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic,
)

@app.agent(input_topic)
async def process_station(stations):
    async for station in stations:
        line = (
            "red" if station.red else(
                "green" if station.green else(
                    "blue" if station.blue else None
                )
            )
        )
        table[station.station_id] = line
        
        transformed_station = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=line
        )
    
        await out_topic.send(value=transformed_station)


if __name__ == "__main__":
    app.main()
