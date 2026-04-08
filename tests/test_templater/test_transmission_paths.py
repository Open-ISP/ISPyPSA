import pandas as pd

from ispypsa.templater.transmission_paths import _template_network_transmission_paths

_FLOW_PATH_COLUMNS = ["Flow Paths"]
_REZ_COLUMNS = ["ID", "Name", "NEM region", "ISP sub-region"]
_OUTPUT_COLUMNS = ["path_id", "geo_from", "geo_to", "carrier"]


def test_template_network_transmission_paths(csv_str_to_df):
    flow_path_transfer_capability = csv_str_to_df("""
        Flow Paths
        CQ-NQ
        NNSW-SQ
        NNSW-SQ (Terranora)
        TAS-SEV
        WNV-CSA (Murraylink)
        CNSW-SNW-NTH
    """)

    renewable_energy_zones = csv_str_to_df("""
        ID,   Name,               NEM region,  ISP sub-region
        Q1,   Far North QLD,      QLD,         NQ
        N3,   Central-West Orana, NSW,         CNSW
    """)

    result = _template_network_transmission_paths(
        flow_path_transfer_capability, renewable_energy_zones
    )

    expected = csv_str_to_df("""
        path_id,             geo_from,  geo_to,  carrier
        CQ-NQ,               CQ,        NQ,      AC
        NNSW-SQ,             NNSW,      SQ,      AC
        NNSW-SQ_Terranora,   NNSW,      SQ,      DC
        TAS-SEV,             TAS,       SEV,     DC
        WNV-CSA_Murraylink,  WNV,       CSA,     DC
        CNSW-SNW_NTH,        CNSW,      SNW,     AC
        Q1-NQ,               Q1,        NQ,      AC
        N3-CNSW,             N3,        CNSW,    AC
    """)

    pd.testing.assert_frame_equal(
        result.sort_values("path_id").reset_index(drop=True),
        expected.sort_values("path_id").reset_index(drop=True),
    )


def test_empty_flow_paths(csv_str_to_df):
    flow_path_transfer_capability = pd.DataFrame(columns=_FLOW_PATH_COLUMNS)

    renewable_energy_zones = csv_str_to_df("""
        ID,   Name,               NEM region,  ISP sub-region
        Q1,   Far North QLD,      QLD,         NQ
    """)

    result = _template_network_transmission_paths(
        flow_path_transfer_capability, renewable_energy_zones
    )

    expected = csv_str_to_df("""
        path_id,  geo_from,  geo_to,  carrier
        Q1-NQ,    Q1,        NQ,      AC
    """)

    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
    )


def test_empty_rez(csv_str_to_df):
    flow_path_transfer_capability = csv_str_to_df("""
        Flow Paths
        CQ-NQ
    """)

    renewable_energy_zones = pd.DataFrame(columns=_REZ_COLUMNS)

    result = _template_network_transmission_paths(
        flow_path_transfer_capability, renewable_energy_zones
    )

    expected = csv_str_to_df("""
        path_id,  geo_from,  geo_to,  carrier
        CQ-NQ,    CQ,        NQ,      AC
    """)

    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
    )


def test_both_empty():
    flow_path_transfer_capability = pd.DataFrame(columns=_FLOW_PATH_COLUMNS)
    renewable_energy_zones = pd.DataFrame(columns=_REZ_COLUMNS)

    result = _template_network_transmission_paths(
        flow_path_transfer_capability, renewable_energy_zones
    )

    expected = pd.DataFrame(columns=_OUTPUT_COLUMNS)

    pd.testing.assert_frame_equal(result, expected)
