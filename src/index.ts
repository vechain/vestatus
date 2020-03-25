#!/usr/bin/env node

import { InfluxDB, IPoint, IWriteOptions } from 'influx'
import Axios from 'axios'
import * as http from 'http'
import * as https from 'https'
import * as URL from 'url'

type ExpandedBlock = {
    number: number
    id: string
    size: number
    parentID: string
    timestamp: number
    gasLimit: number
    beneficiary: string
    gasUsed: number
    totalScore: number
    txsRoot: string
    txsFeatures: number
    stateRoot: string
    receiptsRoot: string
    signer: string
    isTrunk: boolean
    transactions: Array<{
        id: string
        chainTag: number
        blockRef: string
        expiration: number
        clauses: Array<{
            to: string | null
            value: string
            data: string
        }>
        gasPriceCoef: number
        gas: number
        origin: string
        delegator: string | null
        nonce: string
        dependsOn: string | null
        size: number

        // receipt part
        gasUsed: number
        gasPayer: string
        paid: string
        reward: string
        reverted: boolean
        outputs: Array<{
            contractAddress: string | null
            events: Array<{
                address: string
                topics: string[]
                data: string
            }>
            transfers: Array<{
                sender: string
                recipient: string
                amount: string
            }>
        }>
    }>
}

const database = 'vechain'
const measurement = 'blocks'
const writeOptions: IWriteOptions = { precision: 's', database }

const networks: { [index: string]: string } = {
    '0x00000000851caf3cfdb6e899cf5958bfb1ac3413d346d43539627e6be7ec1b4a': 'main',
    '0x000000000b2bce3c70bc649a02749e8687721b09ed2e15997f466536b20bb127': 'test'
}

const nodeURL = process.argv[2] || 'http://localhost:8669'
const influxdbURL = process.argv[3] || 'http://localhost:8086'

async function newBlockFetcher(url: string) {
    const axios = Axios.create({
        httpAgent: new http.Agent({ keepAlive: true }),
        httpsAgent: new https.Agent({ keepAlive: true }),
    })

    const genesis = (await axios.get<{ id: string }>(URL.resolve(url, '/blocks/0'))).data
    const headers = { 'x-genesis-id': genesis.id }
    const getBlock = (n: number | string) => {
        return axios.get<ExpandedBlock | null>(URL.resolve(url, `/blocks/${n}?expanded=true`), { headers }).then(r => r.data)
    }
    const getBlocks = async (start: number, concurrent: number, times: number) => {
        const all = []
        for (let i = 0; i < times; i++) {
            const c = []
            for (let j = 0; j < concurrent; j++) {
                c.push(getBlock(start++))
            }
            all.push(...(await Promise.all(c)))
        }
        return Promise.all(all)
    }
    const network = networks[genesis.id] || 'other'
    return { network, getBlock, getBlocks }
}

function buildRow(network: string, b: ExpandedBlock): IPoint {
    const txs = b.transactions
    return {
        tags: {
            network,
            signer: b.signer,
            beneficiary: b.beneficiary
        },
        fields: {
            num: b.number,
            timestamp: b.timestamp,
            size: b.size,
            gasLimit: b.gasLimit,
            gasUsed: b.gasUsed,
            totalScore: b.totalScore,
            txCount: txs.length,
            clauseCount: txs.reduce((n, tx) => n + tx.clauses.length, 0),
            paid: txs.reduce((n, tx) => n + parseInt(tx.paid), 0),
            reward: txs.reduce((n, tx) => n + parseInt(tx.reward), 0),
            newContractCount: txs.reduce((n, tx) => tx.outputs.reduce((n, o) => n + (o.contractAddress ? 1 : 0), n), 0),
            vip191TxCount: txs.reduce((n, tx) => n + (tx.delegator ? 1 : 0), 0),
            mppTxCount: txs.reduce((n, tx) => n + ((!tx.delegator && tx.origin != tx.gasPayer) ? 1 : 0), 0),
            depTxCount: txs.reduce((n, tx) => n + (tx.dependsOn ? 1 : 0), 0),
            revertedTxCount: txs.reduce((n, tx) => n + (tx.reverted ? 1 : 0), 0),
            transferCount: txs.reduce((n, tx) => tx.outputs.reduce((n, o) => n + o.transfers.length, n), 0),
            eventCount: txs.reduce((n, tx) => tx.outputs.reduce((n, o) => n + o.events.length, n), 0),
            transferAmount: txs.reduce((n, tx) => tx.outputs.reduce((n, o) => o.transfers.reduce((n, t) => n + parseInt(t.amount), n), n), 0)
        },
        timestamp: b.timestamp
    }
}

function sleep(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms))
}

async function sync() {

    const blockFetcher = await newBlockFetcher(nodeURL)
    const db = new InfluxDB(influxdbURL)

    await db.createDatabase(database)
    let start = await db.query<{ max_num: number }>(
        `select last(num) as max_num from blocks where network='${blockFetcher.network}'`,
        { database })
        .then(results => results[0] ? (results[0].max_num || 0) : 0)

    console.log('connected to VeChain network:', blockFetcher.network)

    const timer = setInterval(() => {
        if (process.stdout.clearLine) {
            process.stdout.clearLine(0)
            process.stdout.cursorTo(0)
            process.stdout.write(`imported block ${start}`)
        } else {
            process.stdout.write(`imported block ${start}\n`)
        }
    }, 1000)

    try {
        const times = 20
        const concurrent = 50

        for (; ;) {
            const blocks = await blockFetcher.getBlocks(start, concurrent, times)

            const now = Date.now()
            const confirmedBlocks = blocks.filter(b => {
                if (!b) {
                    return false
                }
                return (b.timestamp + 30) < now / 1000
            }) as ExpandedBlock[]

            if (confirmedBlocks.length > 0) {
                start = confirmedBlocks[confirmedBlocks.length - 1].number + 1
                const rows = confirmedBlocks.map(b => buildRow(blockFetcher.network, b!))
                if (rows.length > 0) {
                    await db.writeMeasurement(measurement, rows, writeOptions)
                }
            }
            if (confirmedBlocks.length < blocks.length) {
                break
            }
        }

        for (; ;) {
            const block = await blockFetcher.getBlock(start)
            let timeToWait = 10 * 1000
            if (block) {
                timeToWait = (block.timestamp + 30) * 1000 - Date.now()
                if (timeToWait < 0) {
                    await db.writeMeasurement(measurement, [buildRow(blockFetcher.network, block)], writeOptions)
                    start = block.number + 1
                }
            }
            if (timeToWait > 0) {
                await sleep(timeToWait)
            }
        }
    } finally {
        clearInterval(timer)
    }
}

(async () => {
    for (; ;) {
        try {
            await sync()
        } catch (err) {
            console.log(err)
        }
        await sleep(20 * 1000)
    }
})()
