from google.cloud import bigquery
from datetime import date, timedelta
import json
client = bigquery.Client()

# query_job = client.query(
#     """
#     SELECT LEFT(data, 64) as data0 FROM `bigquery-public-data.crypto_ethereum.logs` WHERE topics[OFFSET(0)] = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9" LIMIT 10
#     """
# )

# query_job = client.query(
#     """
#     SELECT transaction_hash, topics FROM `bigquery-public-data.crypto_ethereum.logs` 
#     WHERE topics[OFFSET(0)] = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" 
#     AND ARRAY_LENGTH(topics) = 3
#     AND topics[OFFSET(1)] < topics[OFFSET(2)]
#     LIMIT 10
#     """
# )
day = date(2020, 5, 14)
while(day < date(2020, 6, 7)):
    query_job = client.query(
        """
        WITH  swaps AS
          (
            SELECT transaction_hash, STRING_AGG(error) as error, status, RIGHT(to_address, 40) as address, RIGHT(LEFT(input, 202), 40) as recipt, LEFT(input,10) as methods FROM `bigquery-public-data.crypto_polygon.traces` WHERE DATE(block_timestamp) = "{0}" AND LEFT(input,10) = "0x022c0d9f" AND call_type = "call"
            GROUP BY transaction_hash, status, address, recipt, methods
            UNION ALL SELECT transaction_hash, STRING_AGG(error) as error, status, RIGHT(to_address, 40) as address, RIGHT(LEFT(input, 74), 40) as recipt, LEFT(input,10) as methods FROM `bigquery-public-data.crypto_polygon.traces` WHERE DATE(block_timestamp) = "{0}" AND LEFT(input,10) = "0x128acb08" AND call_type = "call"
            GROUP BY transaction_hash, status, address, recipt, methods
          ),
        transfers1 AS
          (
            SELECT transaction_hash, transaction_index, block_hash, block_number, block_timestamp, error, status, RIGHT(to_address, 40) as address, RIGHT(from_address, 40) as sender, RIGHT(LEFT(input, 74), 40) as recipt, RIGHT(input, 64) as value, trace_address FROM `bigquery-public-data.crypto_polygon.traces` WHERE DATE(block_timestamp) = "{0}" AND LEFT(input,10) = "0xa9059cbb" AND call_type = "call"
            UNION ALL SELECT transaction_hash, transaction_index, block_hash, block_number, block_timestamp, error, status, RIGHT(to_address, 40) as address, RIGHT(LEFT(input, 74),40) as sender, RIGHT(LEFT(input, 138), 40) as recipt, RIGHT(input, 64) as value, trace_address FROM `bigquery-public-data.crypto_polygon.traces` WHERE DATE(block_timestamp) = "{0}" AND LEFT(input,10) = "0x23b872dd" AND call_type = "call"
          ),
        transfers0 AS 
          (
            SELECT transaction_hash, transaction_index, block_hash, block_number, block_timestamp, STRING_AGG(error) as error, status, address, sender, recipt, STRING_AGG(value) as values 
            FROM (SELECT * FROM transfers1 ORDER BY block_number, transaction_index, address, sender, recipt, trace_address)
            GROUP BY transaction_hash, transaction_index, block_hash, block_number, block_timestamp, status, address, sender, recipt
          ), 
        transfers AS
        (
            SELECT A.* FROM transfers0 A, (SELECT address FROM swaps GROUP BY address) B
            WHERE A.sender = B.address OR A.recipt = B.address
        ),
        transfersMid AS 
        (
            SELECT block_hash, transaction_hash, transaction_index, error, status, address, sender FROM transfers
            GROUP BY block_hash, transaction_hash, transaction_index, error, status, address, sender
        ),
        allTrxs AS
        (
            SELECT transaction_hash, ARRAY_AGG(CONCAT(sender, address)) as trpair FROM transfers GROUP BY transaction_hash
        ),
        txs AS
          (
            SELECT `hash` as tx_hash, receipt_gas_used, gas_price, from_address, to_address FROM `bigquery-public-data.crypto_polygon.transactions` WHERE DATE(block_timestamp) = "{0}"
          ),
        clipTxs AS
        (
            SELECT * FROM txs
            WHERE RIGHT(to_address, 40) NOT IN ("ef1c6e67703c7bd7107eed8303fbe6ec2554bf6b", "7a250d5630b4cf539739df2c5dacb4c659f2488d", "d9e1ce17f2641f24ae83637ab66a2cca9c378b9f", "f164fc0ec4e93095b804a4795bbe1e041497b92a", "def1c0ded9bec7f1a1670819833240f027b25eff", "91c8d7e4080bed28e26fce1b87caccfaf7bbf794", "1111111254eeb25477b68fb85ed929f73a960582", "1111111254fb6c44bac0bed2854e76f90643097d", "11111112542d85b3ef69ae05771c2dccff4faa26", "68b3465833fb72a70ecdf485e0e4c7bd8665fc45", "e592427a0aece92de3edee1f18e0157c05861564", "7122db0ebe4eb9b434a9f2ffe6760bc03bfbd0e0", "220bda5c8994804ac96ebe4df184d25e5c2196d4", "df1a1b60f2d438842916c0adc43748768353ec25", "111111125434b319222cdbf8c261674adb56f3ae", "11111254369792b2ca5d084ab5eea397ca8fa48b", "84d99aa569d93a9ca187d83734c8c4a519c4e9b1", "a58f22e0766b3764376c92915ba545d583c19dbc", "03f34be1bf910116595db1b11e9d1b2ca5d59659", "dc6c91b569c98f9f6f74d90f9beff99fdaf4248b", "881d40237659c251811cec9c364ef91dc08d300c", "e66b31678d6c16e9ebf358268a790b763c133750")
        ),
        logs0 AS
          (
            SELECT transaction_hash, RIGHT(address,40) as address, RIGHT(topics[OFFSET(1)],40) as sender, RIGHT(topics[OFFSET(2)],40) as recipt, RIGHT(data, 64) as value FROM `bigquery-public-data.crypto_polygon.logs` WHERE DATE(block_timestamp) = "{0}" AND ARRAY_LENGTH(topics) = 3 AND topics[OFFSET(0)] = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
            ORDER BY transaction_hash, address, sender, recipt, log_index
          ),
          logs AS
          (
            SELECT transaction_hash, address, sender, recipt, STRING_AGG(value) as values 
            FROM logs0
            GROUP BY transaction_hash, address, sender, recipt
          ),
        pairs AS
          (
            SELECT A.block_hash as bh, A.block_number as bn, UNIX_SECONDS(A.block_timestamp) as bts, A.sender as pair, A.address as token, A.transaction_hash as tx_hash_a, B.transaction_hash as tx_hash_b, A.transaction_index as tx_id_a, B.transaction_index as tx_id_b, STRING_AGG(A.values) as values_a, STRING_AGG(logA.values) as amounts_a, STRING_AGG(B.values) as values_b, STRING_AGG(logB.values) as amounts_b, B.status as status_b, STRING_AGG(B.error) as err_tr_b
            FROM transfers A, transfers B, swaps swA, logs logA
            LEFT JOIN logs logB
            ON B.transaction_hash = logB.transaction_hash AND B.sender = logB.sender AND B.recipt = logB.recipt AND B.address = logB.address
            WHERE A.status = 1
            AND A.block_hash = B.block_hash
            AND A.transaction_index < B.transaction_index
            AND A.sender = B.recipt AND A.recipt = B.sender AND A.address = B.address
            AND A.transaction_hash = swA.transaction_hash AND A.sender = swA.address AND A.recipt = swA.recipt
            AND A.sender = logA.sender AND A.recipt = logA.recipt AND A.address = logA.address AND A.transaction_hash = logA.transaction_hash
            GROUP BY bh, bn, bts, pair, token, tx_hash_a, tx_hash_b, tx_id_a, tx_id_b, status_b
          ),
        dualPairs AS
          (
            SELECT P.*, nA.address as token0, STRING_AGG(nA.values) as values_na, STRING_AGG(nB.values) as values_nb, STRING_AGG(swB.error) as err_sw_b, STRING_AGG(CAST(swB.status AS STRING)) as status_sw_b
            FROM pairs P, transfers nA
            LEFT JOIN swaps swB
            ON swB.transaction_hash = P.tx_hash_b AND swB.address = P.pair AND swB.recipt = nA.sender
            LEFT JOIN transfers nB
            ON nB.transaction_hash = swB.transaction_hash AND nB.sender = swB.address AND nb.recipt = swB.recipt AND nB.address = nA.address
            WHERE nA.transaction_hash = P.tx_hash_a AND nA.recipt = P.pair
            AND nA.address in ("dac17f958d2ee523a2206206994597c13d831ec7", "b8c77482e45f1f44de1745f52c74426c631bdd52", "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", "0000000000085d4780B73119b644AE5ecd22b376", "4fabb145d64652a948d72533023f6e7a623c7c53", "2260fac5e5542a773aa44fbcfedf7c193bc2c599", "6b175474e89094c44da98b954eedeac495271d0f", "c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")
            GROUP BY bh, bn, bts, pair, token, tx_hash_a, tx_hash_b, tx_id_a, tx_id_b, values_a, amounts_a, values_b, amounts_b, status_b, err_tr_b, token0
          )
        SELECT DP.*, ARRAY_AGG(trM.transaction_hash) as tx_hash_c, ARRAY_AGG(txM.from_address) as from_c, ARRAY_AGG(txM.to_address) as to_c, ARRAY_AGG(trM.status) as status_c, ARRAY_AGG(trM.error IGNORE NULLS) as err_c, ARRAY_AGG(trM.transaction_index) as tx_c_id, ARRAY_AGG(trM.sender) as tx_c_sender, ARRAY_AGG(trM.address) as tx_c_token, txA.receipt_gas_used as gas_a, txA.gas_price as price_a, txB.receipt_gas_used as gas_b, txB.gas_price as price_b, txA.from_address as from_a, txA.to_address as to_a, txB.from_address as from_b, txB.to_address as to_b
        FROM dualPairs DP, transfersMid trM, clipTxs txA, clipTxs txB, txs txM, allTrxs trAA
        WHERE DP.tx_hash_a = txA.tx_hash AND DP.tx_hash_b = txB.tx_hash AND trAA.transaction_hash = txA.tx_hash 
        AND DP.bh = trM.block_hash AND DP.tx_id_a < trM.transaction_index AND trM.transaction_index < DP.tx_id_b
        AND CONCAT(trM.sender,trM.address) in UNNEST(trAA.trpair)
        AND txM.tx_hash = trM.transaction_hash
        GROUP BY bh, bn, bts, pair, token, tx_hash_a, tx_hash_b, tx_id_a, tx_id_b, values_a, amounts_a, values_b, amounts_b, status_b, err_tr_b, token0, values_na, values_nb, err_sw_b, status_sw_b, gas_a, price_a, gas_b, price_b, from_a, to_a, from_b, to_b
        ORDER BY bn, tx_id_a, tx_id_b
        """.format(day)
    )

    results = query_job.result()  # Waits for job to complete.
    results = [dict(row) for row in results]

    with open("data/{}.json".format(day), 'wt') as f:
        str = json.dumps(results)
        f.write(str)
        f.close()
    print("{}".format(day), flush = True)
    day += timedelta(days = 1)