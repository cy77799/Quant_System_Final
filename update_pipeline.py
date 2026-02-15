from layers.data_hub import DataHub
from utils.validation import validate_missing, validate_spikes

def run_update():
    hub = DataHub()

    # 1) 更新價格
    price_df = hub.price.download(force=True)
    close_df = hub.price.load()

    # 2) 技術指標
    tech = hub.build_technical()
    tech.add_rsi(14)
    tech.add_atr(14)
    tech.add_adx(14)
    tech.add_vwap()

    # 3) 基本面
    hub.fundamentals.download_quarterly(hub.price.config['universe']['symbols'])

    # 4) 宏觀
    hub.macro.download_all()

    # 5) 驗證
    missing = validate_missing(close_df)
    spikes = validate_spikes(close_df)

    if not missing.empty:
        print("Missing warning:", missing)
    if spikes > 0:
        print("Spike warning count:", spikes)

if __name__ == "__main__":
    run_update()
