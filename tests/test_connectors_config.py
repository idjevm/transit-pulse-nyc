"""Tests validating Confluent Cloud connector configuration templates."""

import json
from pathlib import Path

CONNECTORS_DIR = Path(__file__).parent.parent / "deploy" / "connectors"


def test_http_sink_dispatcher_decisions_config():
    p = CONNECTORS_DIR / "http_sink_dispatcher_decisions.json"
    assert p.exists()
    cfg = json.loads(p.read_text())
    assert cfg["name"] == "mta-dispatcher-decisions-http-sink"
    c = cfg["config"]
    assert c["connector.class"] == "HttpSink"
    assert c["topics"] == "mta_dispatcher_decisions"
    assert "http.api.url" in c


def test_http_source_weather_config():
    p = CONNECTORS_DIR / "http_source_weather.json"
    assert p.exists()
    cfg = json.loads(p.read_text())
    assert cfg["name"] == "mta-weather-http-source"
    c = cfg["config"]
    assert c["connector.class"] == "HttpSource"
    assert c["topic.name.pattern"] == "nyc_weather_events"
    assert "open-meteo.com" in c["url"]


def test_datagen_passenger_surges_config():
    p = CONNECTORS_DIR / "datagen_passenger_surges.json"
    assert p.exists()
    cfg = json.loads(p.read_text())
    assert cfg["name"] == "mta-passenger-surges-datagen"
    c = cfg["config"]
    assert c["connector.class"] == "DatagenSource"
    assert c["kafka.topic"] == "mta_passenger_surges"
    assert "schema.string" in c
    schema = json.loads(c["schema.string"])
    assert schema["name"] == "PassengerSurge"
    fields = [f["name"] for f in schema["fields"]]
    assert "station_id" in fields
    assert "taps_per_minute" in fields
    assert "crowd_level" in fields

