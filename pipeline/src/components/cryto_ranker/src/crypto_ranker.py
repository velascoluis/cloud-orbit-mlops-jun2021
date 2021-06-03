import argparse
import logging
import cryptowatch as cw
import json
import os
from datetime import datetime, timedelta


def crypto_ranker_last_week(exchange, benefit_threshold, market_list_path):
    logging.basicConfig(level=logging.INFO)
    logging.info('Empieza componente crypto_ranker_last_week ..')
    logging.info('Exchange:{}'.format(exchange))
    logging.info('Benefit threshold:{}'.format(benefit_threshold))
    logging.info('market list path:{}'.format(market_list_path))
    market_list = {}
    selected_markets = cw.markets.list(exchange).markets
    for market in selected_markets:
        try:
            ticker = "{}:{}".format(market.exchange, market.pair).upper()
            candles = cw.markets.get(ticker, ohlc=True, periods=["1w"])
            close_ts, wkly_open, wkly_close = (
                candles.of_1w[-1][0],
                candles.of_1w[-1][1],
                candles.of_1w[-1][4],
            )
            if wkly_open == 0:
                continue
            perf = (wkly_open - wkly_close) * 100 / wkly_open
            if perf >= benefit_threshold:
                open_ts = datetime.utcfromtimestamp(close_ts) - timedelta(days=7)
                logging.info("{} gano {:.2f}% desde {}".format(ticker, perf, open_ts))
                market_list[ticker]= perf
        except:
            logging.info("Exepcion capturada, pero continuamos ..")


    if not os.path.exists(os.path.dirname(market_list_path)):
        os.makedirs(os.path.dirname(market_list_path))
    with open(market_list_path, 'w') as f:
        f.write(json.dumps(market_list))
    logging.info('crypto_ranker_last_week finalizado.')



def main(params):
    crypto_ranker_last_week(params.exchange,
                            params.benefit_threshold,
                            params.market_list_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Conseguimos las mejores crypto monedas de un mercado determinado')
    parser.add_argument('--exchange', type=str, default='coinbase')
    parser.add_argument('--benefit_threshold', type=float, default=20)
    parser.add_argument('--market_list_path', type=str, default='None')
    params = parser.parse_args()
    main(params)
