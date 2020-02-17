# HB
## Data Format

The daily stock data is stored in sqlite3 data base and is stored with each stock as an individual table and each row as an individual trading day.

The columns are:

| Name            | Data Type | Description                                                  |
| --------------- | --------- | ------------------------------------------------------------ |
| trade_date      | str       | date today                                                   |
| open            | Float     | Opening price of the trading day                             |
| high            |           | Highest trading price of the trading day                     |
| low             |           | Lowest trading price of the trading day                      |
| close           |           | Closing price of the trading day                             |
| pre_close       |           | Closing price of last trading day                            |
| pct_chg         |           | percentage change of today's closing price compare to yesterday's closing price |
| change          |           | change of today's closing price compare to yesterday's closing price |
| vol             |           | trading volume(unit is 100 stock or one hand)                |
| amount          |           | trading amount(unit is 1000RMB)                              |
| turnover_rate   |           | trading volume over total share                              |
| turnover_rate_f |           | trading volume over free circulation stock                   |
| volume_ratio    |           |                                                              |
| pe              | float     | PE in last year's earning                                    |
| pe_ttm          | float     | PE in ttm calculation                                        |
| pb              |           | PB in last year's book value                                 |
| ps              | float     | PS in last year's sales                                      |
| ps_ttm          | float     | PS in ttm sales                                              |
| dv_ratio        | float     | dividend ratio in last year's dividend                       |
| dv_ttm          | float     | dividend ratio in ttm calculation                            |
| total_share     | float     | total share(10,000 shares)                                   |
| float_share     | float     | total circulation share(10,000 shares)                       |
| free_share      | float     | total free circulation share(10,000 shares)                  |
| total_mv        | float     | total market value(10,000 RMB)                               |
| circ_mv         | float     | total market value in circulation(10,000 RMB)                |
| adj_factor      | float     | adjustment factor                                            |
| buy_sm_vol      |           | total small buy in volume(100 shares, 1 hand)                |
| buy_sm_amount   |           | total small buy in amount(10,000 RMB)                        |
| sell_sm_vol     |           | total small sell in volume(100 shares, 1 hand)               |
| sell_sm_amount  |           | total small sell in amount(10,000 RMB)                       |
| buy_md_vol      |           | total medium buy in volume(100 shares, 1 hand)               |
| buy_md_amount   |           | total medium buy in amount(10,000 RMB)                       |
| sell_md_vol     |           | total medium sell in volume(100 shares, 1 hand)              |
| sell_md_amount  |           | total medium sell in amount(10,000 RMB)                      |
| buy_lg_vol      |           | total large buy in volume(100 shares, 1 hand)                |
| buy_lg_amount   |           | total large buy in amount(10,000 RMB)                        |
| sell_lg_vol     |           | total large sell in volume(100 shares, 1 hand)               |
| sell_lg_amount  |           | total large sell in amount(10,000 RMB)                       |
| buy_elg_vol     |           | total extra large buy in volume(100 shares, 1 hand)          |
| buy_elg_amount  |           | total extra large buy in amount(10,000 RMB)                  |
| sell_elg_vol    |           | total extra large sell in volume(100 shares, 1 hand)         |
| sell_elg_amount |           | total extra large sell in amount(10,000 RMB)                 |
| net_mf_vol      |           | net flow in volume(100 shares, 1 hand)                       |
| net_mf_amount   |           | net flow in amount(10,000 RMB)                               |
|                 |           |                                                              |
|                 |           |                                                              |
|                 |           |                                                              |
|                 |           |                                                              |
|                 |           |                                                              |
|                 |           |                                                              |
|                 |           |                                                              |

