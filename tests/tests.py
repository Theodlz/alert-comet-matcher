import pytest

from alert_comets.utils.validate import (
    MovingObject,
    ObjectWithPosition,
    ObjectsWithPosition,
    KowalskiCredentials,
)
from alert_comets.utils.moving_objects import get_object_positions
from alert_comets.utils.comets import get_comets_list
from alert_comets.utils.kowalski import build_cone_search


def test_validate_kowalski_credentials():
    try:
        credentials = KowalskiCredentials(
            protocol="https", host="kowalski.caltech.edu", port=443, token="token"
        )
    except Exception as e:
        assert False, f"Exception raised: {str(e)}"

    assert isinstance(credentials, KowalskiCredentials)

    credentials_dict = credentials.to_dict()
    assert isinstance(credentials_dict, dict)
    assert len(credentials_dict) == 4
    assert credentials_dict["protocol"] == "https"
    assert credentials_dict["host"] == "kowalski.caltech.edu"
    assert credentials_dict["port"] == 443
    assert credentials_dict["token"] == "token"


def test_validate_object_with_position():
    try:
        obj = ObjectWithPosition(name="ZTF21abjyjxw", ra=1.0, dec=2.0)
    except Exception as e:
        assert False, f"Exception raised: {str(e)}"

    assert isinstance(obj, ObjectWithPosition)

    obj_dict = obj.to_dict()
    assert isinstance(obj_dict, dict)
    assert len(obj_dict) == 3
    assert obj_dict["name"] == "ZTF21abjyjxw"
    assert obj_dict["ra"] == 1.0
    assert obj_dict["dec"] == 2.0


def test_validate_objects_with_position():
    try:
        obj = ObjectWithPosition(name="ZTF21abjyjxw", ra=1.0, dec=2.0)
        objs = ObjectsWithPosition(objects_with_position=[obj])
    except Exception as e:
        assert False, f"Exception raised: {str(e)}"

    assert isinstance(objs, ObjectsWithPosition)

    objs_dict = objs.to_dict()
    assert isinstance(objs_dict, dict)
    assert len(objs_dict) == 1
    assert "ZTF21abjyjxw" in objs_dict
    assert len(objs_dict["ZTF21abjyjxw"]) == 2
    assert objs_dict["ZTF21abjyjxw"]["ra"] == 1.0
    assert objs_dict["ZTF21abjyjxw"]["dec"] == 2.0


def test_validate_moving_object():
    try:
        obj = MovingObject(name="C/2020 F3", ra=[1, 2, 3], dec=[4, 5, 6], jd=[7, 8, 9])
    except Exception as e:
        assert False, f"Exception raised: {str(e)}"

    assert isinstance(obj, MovingObject)

    obj_dict = obj.to_dict()
    assert isinstance(obj_dict, dict)
    assert len(obj_dict) == 4
    assert obj_dict["name"] == "C/2020 F3"
    assert obj_dict["ra"] == [1, 2, 3]
    assert obj_dict["dec"] == [4, 5, 6]
    assert obj_dict["jd"] == [7, 8, 9]


def test_get_objects_positions():
    try:
        positions = get_object_positions("C/2020 F3", "2023-07-30", "2023-07-31")
    except Exception as e:
        assert False, f"Exception raised: {str(e)}"

    assert isinstance(positions, dict)

    # the validator if anything is incorrect
    try:
        MovingObject(name="C/2020 F3", **positions)
    except Exception as e:
        assert False, f"MovingObject validator raised an exception: {str(e)}"

    # verify that ra, dec, and jd are not empty
    assert all(
        [len(v) > 0 for v in positions.values()]
    ), "ra, dec, and jd must not be empty"


def test_get_comets_list():
    try:
        comets = get_comets_list(source="yfernandez")
    except Exception as e:
        pytest.fail(f"Exception raised: {str(e)}")

    assert isinstance(comets, list)
    assert len(comets) > 0

    # verify that all comets are strings
    assert all(
        [isinstance(comet, str) for comet in comets]
    ), "all comet names must be strings"

    # verify that the list has more than 1000 comets
    assert len(comets) > 662, "the list should have at least 663 comets"


def test_build_cone_search():
    try:
        obj = ObjectWithPosition(name="ZTF21abjyjxw", ra=1.0, dec=2.0)
        objs = ObjectsWithPosition(objects_with_position=[obj])
        query = build_cone_search(
            objects_with_position=objs.to_dict(coordinates_as_list=True),
            catalogs_parameters={
                "ZTF_alerts": {
                    "filter": {"candidate.drb": {"$gt": 0.5}},
                    "projection": {"candidate.drb": 1},
                }
            },
        )
    except Exception as e:
        assert False, f"Exception raised: {str(e)}"

    assert isinstance(query, dict)
    assert "query_type" in query
    assert query["query_type"] == "cone_search"
    assert "query" in query
    assert isinstance(query["query"], dict)

    assert "object_coordinates" in query["query"]
    assert isinstance(query["query"]["object_coordinates"], dict)
    assert "cone_search_radius" in query["query"]["object_coordinates"]
    assert isinstance(query["query"]["object_coordinates"]["cone_search_radius"], float)
    assert query["query"]["object_coordinates"]["cone_search_radius"] == 5.0
    assert "cone_search_unit" in query["query"]["object_coordinates"]
    assert isinstance(query["query"]["object_coordinates"]["cone_search_unit"], str)
    assert query["query"]["object_coordinates"]["cone_search_unit"] == "arcsec"

    assert "radec" in query["query"]["object_coordinates"]
    assert isinstance(query["query"]["object_coordinates"]["radec"], dict)

    assert "ZTF21abjyjxw" in query["query"]["object_coordinates"]["radec"]
    assert isinstance(
        query["query"]["object_coordinates"]["radec"]["ZTF21abjyjxw"], list
    )
    assert len(query["query"]["object_coordinates"]["radec"]["ZTF21abjyjxw"]) == 2
    assert query["query"]["object_coordinates"]["radec"]["ZTF21abjyjxw"][0] == 1.0
    assert query["query"]["object_coordinates"]["radec"]["ZTF21abjyjxw"][1] == 2.0

    assert "catalogs" in query["query"]
    assert isinstance(query["query"]["catalogs"], dict)

    assert "ZTF_alerts" in query["query"]["catalogs"]
    assert isinstance(query["query"]["catalogs"]["ZTF_alerts"], dict)

    assert "filter" in query["query"]["catalogs"]["ZTF_alerts"]
    assert isinstance(query["query"]["catalogs"]["ZTF_alerts"]["filter"], dict)
    assert "candidate.drb" in query["query"]["catalogs"]["ZTF_alerts"]["filter"]
    assert isinstance(
        query["query"]["catalogs"]["ZTF_alerts"]["filter"]["candidate.drb"], dict
    )
    assert "$gt" in query["query"]["catalogs"]["ZTF_alerts"]["filter"]["candidate.drb"]
    assert (
        query["query"]["catalogs"]["ZTF_alerts"]["filter"]["candidate.drb"]["$gt"]
        == 0.5
    )

    assert "projection" in query["query"]["catalogs"]["ZTF_alerts"]
    assert isinstance(query["query"]["catalogs"]["ZTF_alerts"]["projection"], dict)
    assert "candidate.drb" in query["query"]["catalogs"]["ZTF_alerts"]["projection"]
    assert query["query"]["catalogs"]["ZTF_alerts"]["projection"]["candidate.drb"] == 1


def test_connect_kowalski():
    # for now we skip, because we need to setup the tests so that they can query a test Kowalski instance
    pytest.skip("skipping test_connect_kowalski")


def test_run_queries():
    # for now we skip, because we need to setup the tests so that they can query a test Kowalski instance
    pytest.skip("skipping test_run_queries")
