import json
from pathlib import Path

def test_flows_vs_pressure_alignment():
    flows = json.loads(Path('sample_data/flows_btc.json').read_text())
    press = json.loads(Path('sample_data/pressure_btc.json').read_text())
    v_flow  = float(flows['volume_ratio'])          # single source of truth we want
    v_press = float(press['components']['vol_ratio'])
    # Fail if flows say OK but pressure would block
    assert not (v_flow >= 0.80 and v_press < 0.70), (
        f'Contradiction: flows.volume_ratio={v_flow} vs pressure.vol_ratio={v_press}'
    )
