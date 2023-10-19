import apache_beam as beam
import pytest
from apache_beam.testing import test_pipeline
from apache_beam.testing.util import assert_that, equal_to
from scale_aware_air_sea.beam import AirSeaPaper


runners = ["DirectRunner"]
runner_ids = ["DirectRunner"]


@pytest.fixture
def input_expected():
    input = [("cesm", "cesm_ds"), ("cm26", "cm26_ds")]
    expected = [
        ("cesm", "$cesm_ds_arco+cesm_ds_arco_filtered$_a,b,c_flux"),
        ("cm26", "$cm26_ds_arco+cm26_ds_arco_filtered$_a,b,c_flux"),
        ("cesm", "$cesm_ds_arco+cesm_ds_arco_filtered$_a,bf,cf_flux"),
        ("cm26", "$cm26_ds_arco+cm26_ds_arco_filtered$_a,bf,cf_flux"),
        ("cesm", "$cesm_ds_arco+cesm_ds_arco_filtered$_a,b,cf_flux"),
        ("cm26", "$cm26_ds_arco+cm26_ds_arco_filtered$_a,b,cf_flux"),
    ]
    return input, expected


@pytest.mark.parametrize("runner", runners, ids=runner_ids)
def test_zero_equals_one(runner, input_expected):
    input, expected = input_expected
    with test_pipeline.TestPipeline(runner=runner) as p:
        pcoll = p | beam.Create(input) | AirSeaPaper()
        assert_that(pcoll, equal_to(expected))
