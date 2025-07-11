import ray
from src.utils.moving_objects import get_object_positions

# connect to the Ray cluster
ray.init()


@ray.remote
def f():
    return 1


@ray.remote
def ray_get_object_positions(obj_name, start_date, end_date, time_step, verbose):
    return get_object_positions(
        obj_name=obj_name,
        start_date=start_date,
        end_date=end_date,
        time_step=time_step,
        verbose=verbose,
    )


def test_ray():
    result = f.remote()
    assert ray.get(result) == 1


def test_get_objects_positions():
    obj_name = "C/2020 F3"
    start_date = "2020-01-01"
    end_date = "2020-01-02"
    time_step = "10m"
    verbose = False
    try:
        result = ray_get_object_positions.remote(
            obj_name=obj_name,
            start_date=start_date,
            end_date=end_date,
            time_step=time_step,
            verbose=verbose,
        )
        result = ray.get(result)
    except Exception as e:
        assert False, f"Exception raised: {str(e)}"

    assert isinstance(result, dict)
    assert len(result) == 3
    assert "ra" in result
    assert "dec" in result
    assert "jd" in result
    assert isinstance(result["ra"], list)
    assert isinstance(result["dec"], list)
    assert isinstance(result["jd"], list)
    assert len(result["ra"]) == len(result["dec"]) == len(result["jd"])
    assert len(result["ra"]) > 0
