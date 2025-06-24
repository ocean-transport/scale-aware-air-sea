from dataclasses import dataclass
import apache_beam as beam

input_data = [("cesm", "cesm_ds"), ("cm26", "cm26_ds")]


def append_val(t: tuple, val: str) -> tuple:
    k, v = t
    return (k, v + val)


class PangeoForgeRecipe(beam.PTransform):
    def expand(self, pcoll: beam.PCollection[tuple]):
        return pcoll | beam.Map(append_val, val="_arco")


class XBeamFilter(beam.PTransform):
    def expand(self, pcoll: beam.PCollection[tuple]):
        return pcoll | beam.Map(append_val, val="_filtered")


def add_spec(t: tuple, spec: str):
    k, v = t
    merged = "+".join(list(v))
    return (k, f"${merged}$_{spec}")


@dataclass
class MixVariables(beam.PTransform):
    spec: str

    def expand(self, pcoll: beam.PCollection[tuple]):
        return pcoll | beam.Map(add_spec, spec=self.spec)


def flatten_tuple(t: tuple):
    k, v = t
    arco, filtered = v
    return (k, (arco[0], filtered[0]))


class XBeamComputeFluxes(beam.PTransform):
    def expand(self, pcoll: beam.PCollection[tuple]):
        # return pcoll | xbeam.Something()
        return pcoll | beam.Map(append_val, val="_flux")


class AirSeaPaper(beam.PTransform):
    def expand(self, pcoll):
        arco = pcoll | PangeoForgeRecipe()  # -> Zarr(data_vars={a, b, c})
        filtered = arco | XBeamFilter()  # -> Zarr(data_vars={a_f, b_f, c_f})
        nested = (arco, filtered) | beam.CoGroupByKey() | beam.Map(flatten_tuple)

        a_b_c = nested | "mix 0" >> MixVariables(spec="a,b,c")
        a_b_cf = nested | "mix 1" >> MixVariables(spec="a,b,cf")
        a_bf_cf = nested | "mix 2" >> MixVariables(spec="a,bf,cf")
        fluxes = (a_b_c, a_b_cf, a_bf_cf) | beam.Flatten() | XBeamComputeFluxes()
        return fluxes


if __name__ == "__main__":
    with beam.Pipeline() as p:
        p | beam.Create(input_data) | AirSeaPaper() | beam.Map(print)
